from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("days_since_last_restock", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("product_id")
w3 = Window.orderBy("days_since_last_restock", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity_avg_5_prior").desc(), "id")
w5 = Window.partitionBy("store_id", "product_id").orderBy("quantity_avg_5_prior", "id")
w6 = Window.partitionBy("product_id", "store_id").orderBy("quantity_avg_5_prior", "id")
w7 = Window.partitionBy(F.year("last_restock"), "product_id")
w8 = Window.partitionBy(F.year("last_restock"), "store_id").orderBy("quantity_avg_5_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy(F.date_trunc("month", "last_restock"), "product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("store_id").orderBy("last_restock", "id")
w11 = Window.partitionBy("store_id").orderBy(F.col("prev_product_id_stddev_samp_quantity").desc_nulls_last(), "id")

filtered = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2023-08-21").cast("date")) & (F.col("last_restock") <= "2023-06-01"))
    .select(
        "*",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
)

metrics = (
    filtered
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.avg("quantity").over(w).alias("quantity_avg_5_prior"),
        F.stddev_samp("quantity").over(w2).alias("product_id_stddev_samp_quantity"),
        F.avg("days_since_last_restock").over(w3).alias("days_since_last_restock_avg_5_prior"),
    )
)

latest = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

scored = (
    latest
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        "product_id_stddev_samp_quantity",
        "days_since_last_restock_avg_5_prior",
        F.lag("product_id_stddev_samp_quantity", 1).over(w5).alias("prev_product_id_stddev_samp_quantity"),
        F.ntile(3).over(w6).alias("quantity_avg_5_prior_bucket"),
        F.sum("days_since_last_restock_avg_5_prior").over(w7).alias("last_restock_year_sum_days_since_last_restock_avg_5_prior"),
        F.sum("product_id_stddev_samp_quantity").over(w8).alias("cumulative_product_id_stddev_samp_quantity"),
        F.first("quantity_avg_5_prior", ignorenulls=True).over(w_running).alias("first_known_quantity_avg_5_prior"),
    )
)

enriched = (
    scored
    .withColumn("next_days_since_last_restock_avg_5_prior", F.lead("days_since_last_restock_avg_5_prior").over(w_grp))
    .withColumn("prev_days_since_last_restock_avg_5_prior", F.lag("days_since_last_restock_avg_5_prior").over(w_grp))
    .withColumn("next_first_known_quantity_avg_5_prior", F.lead("first_known_quantity_avg_5_prior").over(w_grp))
    .withColumn("prev_first_known_quantity_avg_5_prior", F.lag("first_known_quantity_avg_5_prior").over(w_grp))
    .withColumn("event_seq", F.row_number().over(w_grp))
)

deduped = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w11))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    deduped
    .groupBy(F.date_trunc("YEAR", "last_restock").alias("year_start"))
    .agg(
        F.round(F.avg("prev_days_since_last_restock_avg_5_prior"), 2).alias("avg_prev_days_since_last_restock_avg_5_prior"),
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq"),
        F.round(F.avg("days_since_last_restock_avg_5_prior"), 2).alias("avg_days_since_last_restock_avg_5_prior"),
        F.round(F.avg("first_known_quantity_avg_5_prior"), 2).alias("avg_first_known_quantity_avg_5_prior"),
        F.count("*").alias("num_rows"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
    )
    .orderBy(F.col("num_rows").desc())
)
