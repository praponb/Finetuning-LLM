from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("region").orderBy(F.col("shipping_fee").asc_nulls_last())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())

cleaned = (
    df_stores
    .filter(F.col("opened_date") > F.lit("2023-04-13").cast("date"))
    .withColumn("sqft_level", F.when(F.col("sqft") >= 16482, "High").when(F.col("sqft") >= 4338, "Medium").otherwise("Low"))
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.order_date").alias("order_order_date"),
        F.col("c.shipping_fee"),
    )
)

ranked = (
    with_latest
    .withColumn("row_num", F.rank().over(w2))
    .filter(F.col("row_num") == 1)
    .select("id", "store_name", "opened_date", "order_order_date", "shipping_fee")
)

latest_match = (
    ranked
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") <= F.col("s.order_order_date")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.revenue"))
)

cleaned_stores = (
    latest_match
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-09-09"))) & (F.col("opened_date") < "2025-08-01"))
    .select(
        "id",
        "store_name",
        "order_order_date",
        "revenue",
        F.coalesce("store_name", F.lit("Unknown")).alias("store_name_filled"),
    )
)

result = (
    cleaned_stores
    .groupBy(
        F.when(F.col("revenue") >= 3700, "LARGE").otherwise("SMALL").alias("revenue_tier"),
        F.date_trunc("WEEK", "order_order_date").alias("week_start"),
    )
    .agg(
        F.percentile_approx("revenue", 0.95).alias("revenue_p95"),
        F.percentile_approx("revenue", [0.25, 0.5, 0.75]).alias("revenue_quartiles"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("revenue", [0.25, 0.5, 0.75]).alias("revenue_quartiles_2"),
        F.avg("revenue").alias("avg_revenue"),
    )
)
