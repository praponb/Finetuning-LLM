from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue", "id")
w2 = Window.partitionBy(F.month("sale_date")).orderBy("revenue_scaled_times_revenue", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("next_revenue_scaled", "id")
w5 = Window.orderBy(F.col("revenue_scaled_avg_5").desc())

valid = (
    df_store_sales
    .filter(F.col("product_id") == 6)
    .select("*", F.round((F.col("revenue") * 1.15), 2).alias("revenue_scaled"))
)

cleaned = (
    valid
    .filter(F.col("store_id").isin([1, 2, 5, 6]))
    .withColumn("revenue_scaled_times_revenue", (F.col("revenue_scaled") * F.col("revenue")))
)

scored = (
    cleaned
    .filter(F.col("revenue_scaled_times_revenue") > 31)
    .select(
        "id",
        "revenue",
        F.lead("revenue_scaled").over(w).alias("next_revenue_scaled"),
        F.lag("revenue_scaled").over(w).alias("prev_revenue_scaled"),
        F.lead("revenue_scaled_times_revenue").over(w).alias("next_revenue_scaled_times_revenue"),
        F.row_number().over(w).alias("event_seq"),
        F.avg("revenue_scaled").over(w2).alias("revenue_scaled_avg_5"),
    )
)

enriched = (
    scored
    .withColumn("first_known_prev_revenue_scaled", F.first("prev_revenue_scaled", ignorenulls=True).over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
    .withColumn("revenue_scaled_avg_5_dense_rank", F.dense_rank().over(w5))
)

base = (
    enriched
    .filter(F.col("prev_revenue_scaled").between(26, 58))
    .select(
        "*",
        F.ceil("revenue_scaled_avg_5_dense_rank").alias("revenue_scaled_avg_5_dense_rank_whole"),
    )
)

result = (
    base
    .groupBy("seq_no")
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("next_revenue_scaled", [0.25, 0.5, 0.75]).alias("next_revenue_scaled_quartiles"),
    )
)
