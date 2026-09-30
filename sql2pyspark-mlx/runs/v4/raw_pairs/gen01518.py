from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc())
w2 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.orderBy("quantity", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w5 = Window.partitionBy("product_id", "store_id")
w6 = Window.partitionBy("store_id", "product_id").orderBy("quantity_avg_5")
w7 = Window.partitionBy("store_id").orderBy("quantity_avg_5", "id").rowsBetween(-3, Window.currentRow)
w8 = Window.partitionBy("store_id").orderBy("quantity_avg_5_cume_dist", "id")
w9 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy(F.col("product_id_min_quantity_avg_5").desc(), "id")
w12 = Window.partitionBy("store_id", "product_id").orderBy(F.col("dedup_rank").desc(), "id")

scored = (
    df_inventory
    .select(
        "*",
        F.percent_rank().over(w).alias("quantity_percent_rank"),
        F.avg("quantity").over(w2).alias("quantity_avg_5"),
        F.avg("quantity").over(w3).alias("quantity_avg_2_prior"),
    )
)

deduped = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    deduped
    .select(
        "*",
        F.min("quantity_avg_5").over(w5).alias("product_id_min_quantity_avg_5"),
        F.cume_dist().over(w6).alias("quantity_avg_5_cume_dist"),
    )
)

metrics_inventory = (
    metrics
    .select(
        "*",
        F.avg("quantity_avg_2_prior").over(w7).alias("quantity_avg_2_prior_avg_3"),
        F.ntile(5).over(w8).alias("quantity_avg_5_cume_dist_bucket"),
        F.sum("quantity").over(w9).alias("running_quantity"),
    )
)

cleaned = (
    metrics_inventory
    .filter(F.col("last_restock") > F.lit("2024-02-10").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        "quantity_percent_rank",
        "quantity_avg_5",
        "quantity_avg_2_prior",
        "dedup_rank",
        "product_id_min_quantity_avg_5",
        "quantity_avg_5_cume_dist",
        "quantity_avg_2_prior_avg_3",
        "quantity_avg_5_cume_dist_bucket",
        "running_quantity",
        F.col("quantity_avg_5_cume_dist").cast("int").alias("quantity_avg_5_cume_dist_int"),
        (F.col("quantity_avg_5_cume_dist") / F.nullif("quantity_avg_5", F.lit(0))).alias("quantity_avg_5_cume_dist_per_quantity_avg_5"),
    )
)

scored_inventory = (
    cleaned
    .withColumn("first_known_quantity_avg_2_prior", F.first("quantity_avg_2_prior", ignorenulls=True).over(w10))
    .withColumn("event_seq", F.row_number().over(w_recent))
)

latest = (
    scored_inventory
    .withColumn("rank_in_group", F.row_number().over(w12))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    latest
    .groupBy("store_id")
    .agg(
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.percentile_approx("quantity_avg_5_cume_dist", [0.25, 0.5, 0.75]).alias("quantity_avg_5_cume_dist_quartiles"),
        F.percentile_approx("quantity_percent_rank", [0.25, 0.5, 0.75]).alias("quantity_percent_rank_quartiles"),
        F.sum("quantity_percent_rank").alias("total_quantity_percent_rank"),
        F.percentile_approx("quantity_avg_5_cume_dist_bucket", [0.25, 0.5, 0.75]).alias("quantity_avg_5_cume_dist_bucket_quartiles"),
        F.round(F.avg("quantity_percent_rank"), 2).alias("avg_quantity_percent_rank"),
    )
    .orderBy("distinct_store_ids")
)
