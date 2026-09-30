from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("product_id", "store_id").orderBy("revenue", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy(F.year("sale_date"), "store_id").orderBy("units", "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("last3_count_revenue").desc(), "id")
w4 = Window.partitionBy("product_id").orderBy(F.col("last3_sum_revenue").desc_nulls_first())
w5 = Window.partitionBy("product_id").orderBy(F.col("units").desc(), "id")
w6 = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("product_id").orderBy("last3_max_revenue_scaled", "id")
w8 = Window.orderBy(F.col("greatest_event_seq_units").desc(), "id")
w9 = Window.partitionBy(F.month("sale_date"), "product_id")

scored = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.max("revenue").over(w_recent).alias("last3_max_revenue"),
        F.count("revenue").over(w_recent).alias("last3_count_revenue"),
        F.sum("revenue").over(w_recent).alias("last3_sum_revenue"),
        F.min("revenue").over(w_recent).alias("last3_min_revenue"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

deduped = (
    scored
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

valid = (
    deduped
    .filter(F.col("sale_date") >= F.lit("2023-08-27").cast("date"))
    .withColumn("greatest_event_seq_units", F.greatest("event_seq", "units"))
    .withColumn("last3_max_revenue_scaled", F.round((F.col("last3_max_revenue") * 0.85), 2))
)

scored_store_sales = (
    valid
    .filter(F.month("sale_date").isin([4, 8, 9]))
    .withColumn("last3_sum_revenue_dense_rank", F.dense_rank().over(w4))
    .withColumn("units_bucket", F.ntile(4).over(w5))
    .withColumn("running_revenue", F.sum("revenue").over(w6))
    .withColumn("prev_last3_sum_revenue", F.lag("last3_sum_revenue", 2).over(w_running))
)

enriched = (
    scored_store_sales
    .select(
        "*",
        F.row_number().over(w8).alias("seq_no"),
        F.round((F.col("last3_sum_revenue_dense_rank") / F.sum("last3_sum_revenue_dense_rank").over(w9)), 4).alias("last3_sum_revenue_dense_rank_share"),
    )
)

result = (
    enriched
    .groupBy(F.date_trunc("QUARTER", "sale_date").alias("quarter_start"))
    .agg(
        F.percentile_approx("last3_count_revenue", [0.25, 0.5, 0.75]).alias("last3_count_revenue_quartiles"),
        F.count("*").alias("event_count"),
        F.max("running_revenue").alias("max_running_revenue"),
    )
)
