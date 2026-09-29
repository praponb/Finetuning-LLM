from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("sqft", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w3 = Window.orderBy("revenue", "id")
w_grp = Window.orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp_2 = Window.orderBy(F.col("revenue").desc(), "id")

base = (
    df_stores
    .filter(F.col("region").isNotNull() & F.col("city").isin(["Singapore", "Berlin"]))
    .select(
        "*",
        F.upper(F.trim("store_name")).alias("store_name_normalized"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("region_filled"),
    )
)

ranked = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "opened_date", "sqft", "region_filled")
)

base_stores = (
    ranked
    .filter((F.col("opened_date") >= F.lit("2023-06-02").cast("date")) & (F.col("sqft") < 15638))
    .withColumn("sqft_tier", F.when(F.col("sqft") >= 13064, "High").when(F.col("sqft") >= 8039, "Medium").otherwise("Low"))
    .withColumn("days_since_opened_date", F.datediff(F.current_date(), "opened_date"))
)

with_last = (
    base_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.region_filled", F.col("c.revenue"))
)

scored = (
    with_last
    .filter(F.col("revenue").isNotNull())
    .withColumn("prev_revenue", F.lag("revenue", 2).over(w3))
    .withColumn("cumulative_revenue", F.sum("revenue").over(w_grp))
    .withColumn("revenue_bucket", F.ntile(5).over(w_grp_2))
    .withColumn("last_known_revenue", F.last("revenue", ignorenulls=True).over(w_grp))
)

kept = (
    scored
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.sale_date").between("2023-04-01", "2024-06-01")), "left_semi")
)

summary = (
    kept
    .withColumn("prev_revenue_tier", F.when(F.col("prev_revenue") >= 3600, "PREMIUM").otherwise("STANDARD"))
    .withColumn("cumulative_revenue_tier", F.when(F.col("cumulative_revenue") >= 22140, "LARGE").otherwise("SMALL"))
    .cube("prev_revenue_tier", "cumulative_revenue_tier")
    .agg(
        F.count("*").alias("event_count"),
        F.sum("prev_revenue").alias("total_prev_revenue"),
        F.percentile_approx("cumulative_revenue", 0.95).alias("cumulative_revenue_p95"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.percentile_approx("revenue", 0.9).alias("revenue_p90"),
        F.round(F.avg("cumulative_revenue"), 2).alias("avg_cumulative_revenue"),
        F.grouping("prev_revenue_tier").alias("grouping_level"),
    )
)

result = (
    summary
    .select(
        "event_count",
        "total_prev_revenue",
        "revenue_p90",
        "avg_cumulative_revenue",
    )
    .orderBy(F.col("event_count").asc_nulls_last())
)
