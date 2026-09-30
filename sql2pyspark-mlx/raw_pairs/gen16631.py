from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity_scaled").desc_nulls_first())
w2 = Window.partitionBy(F.year("last_restock"), "store_id").orderBy("last_restock", "id").rowsBetween(-3, -1)
w3 = Window.orderBy(F.col("quantity_scaled").desc(), "id")
w4 = Window.partitionBy("store_id")
w5 = Window.orderBy("quantity_scaled_avg_3_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("quantity_scaled_bucket", "id")
w7 = Window.orderBy(F.col("quantity_scaled_rank").desc_nulls_first())
w8 = Window.orderBy("quantity_scaled_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-02-09").cast("date"))
    .select("*", F.round((F.col("quantity") * 0.9), 2).alias("quantity_scaled"))
)

scored = (
    valid
    .filter(F.col("last_restock") >= "2024-07-01")
    .select(
        "id",
        "quantity",
        F.rank().over(w).alias("quantity_scaled_rank"),
        F.avg("quantity_scaled").over(w2).alias("quantity_scaled_avg_3_prior"),
        F.ntile(3).over(w3).alias("quantity_scaled_bucket"),
        F.sum("quantity_scaled").over(w4).alias("store_id_sum_quantity_scaled"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.first("quantity_scaled_rank", ignorenulls=True).over(w5).alias("first_known_quantity_scaled_rank"),
        F.ntile(4).over(w6).alias("quantity_scaled_bucket_bucket"),
        F.rank().over(w7).alias("quantity_scaled_rank_rank"),
        F.sum("quantity_scaled_bucket").over(w8).alias("cumulative_quantity_scaled_bucket"),
        F.sum("quantity").over(w9).alias("running_quantity"),
    )
)

valid_inventory = (
    with_windows
    .filter(F.col("first_known_quantity_scaled_rank") != 5)
    .select(
        "id",
        "quantity",
        "quantity_scaled_rank",
        "quantity_scaled_avg_3_prior",
        "quantity_scaled_bucket",
        "store_id_sum_quantity_scaled",
        "first_known_quantity_scaled_rank",
        "quantity_scaled_rank_rank",
        "running_quantity",
        (F.col("quantity_scaled_rank_rank") / F.nullif("quantity_scaled_avg_3_prior", F.lit(0))).alias("quantity_scaled_rank_rank_per_quantity_scaled_avg_3_prior"),
    )
)

metrics = (
    valid_inventory
    .select(
        "*",
        F.lag("running_quantity").over(w6).alias("prev_running_quantity"),
        F.lag("quantity_scaled_rank_rank").over(w6).alias("prev_quantity_scaled_rank_rank"),
        F.lead("quantity_scaled_rank_rank").over(w6).alias("next_quantity_scaled_rank_rank"),
        F.row_number().over(w6).alias("row_seq"),
    )
)

totals = (
    metrics
    .withColumn("row_seq_tier", F.when(F.col("row_seq") >= 9, "HIGH").otherwise("LOW"))
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 235, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["row_seq_tier", "quantity_tier"], ["row_seq_tier"], []],
        "row_seq_tier",
        "quantity_tier",
    )
    .agg(
        F.round(F.avg("first_known_quantity_scaled_rank"), 2).alias("avg_first_known_quantity_scaled_rank"),
        F.count("*").alias("cnt"),
        F.round(F.avg("quantity_scaled_rank"), 2).alias("avg_quantity_scaled_rank"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("quantity_scaled_bucket", 0.5).alias("quantity_scaled_bucket_p50"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    totals
    .select(
        "row_seq_tier",
        "quantity_tier",
        "cnt",
        "avg_quantity_scaled_rank",
        "quantity_scaled_bucket_p50",
        "gid",
    )
    .orderBy("cnt")
)
