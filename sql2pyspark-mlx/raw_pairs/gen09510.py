from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(-6, Window.currentRow)

scored = (
    df_store_sales
    .filter(F.col("sale_date") < "2025-09-01")
    .withColumn("last7_count_revenue", F.count("revenue").over(w_recent))
    .withColumn("last7_sum_revenue", F.sum("revenue").over(w_recent))
    .withColumn("last7_avg_revenue", F.avg("revenue").over(w_recent))
)

cleaned = (
    scored
    .filter(F.col("sale_date") >= F.lit("2023-03-02").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "last7_count_revenue",
        "last7_sum_revenue",
        "last7_avg_revenue",
        F.date_sub("sale_date", 59).alias("sale_date_minus_59d"),
    )
)

cleaned_store_sales = (
    cleaned
    .filter(F.col("sale_date") >= F.lit("2023-09-11").cast("date"))
    .select(
        "id",
        "store_id",
        "sale_date",
        "revenue",
        "last7_count_revenue",
        F.col("units").cast("double").alias("units_num"),
    )
)

with_dims = (
    cleaned_store_sales
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.city"), F.col("b.region"))
)

kept_keys = (
    with_dims
    .select("store_id")
    .intersect(
        df_store_sales
        .filter(F.col("sale_date") >= "2025-04-28")
        .select("store_id")
    )
)

verified = (
    with_dims
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("activity_month"))
    .agg(
        F.round(F.sum("revenue") / F.nullif(F.count(F.when(F.col("city") == "Chiang Mai", 1)), F.lit(0)), 2).alias("revenue_per_chiang_mai"),
        F.percentile_approx("last7_count_revenue", [0.25, 0.5, 0.75]).alias("last7_count_revenue_quartiles"),
        F.max(F.when(F.col("city") == "Tokyo", F.col("units_num"))).alias("tokyo_max_units_num"),
        F.sum(F.when(F.col("city") == "London", F.col("revenue")).otherwise(0)).alias("london_revenue"),
    )
)
