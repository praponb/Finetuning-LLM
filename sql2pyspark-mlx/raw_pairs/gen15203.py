from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())

cleaned = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-09-06").cast("date")) & F.col("opened_date").between("2023-08-01", "2023-11-01"))
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft",
        F.when(F.col("sqft") >= 14106, "High").when(F.col("sqft") >= 6032, "Medium").otherwise("Low").alias("sqft_level"),
    )
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.region",
        "s.sqft_level",
        F.col("c.status").alias("order_status"),
        F.col("c.channel").alias("order_channel"),
    )
)

kept = (
    with_latest
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.units").between(19, 39)), "left_semi")
)

with_latest_stores = (
    kept
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date"), F.col("c.revenue").alias("store_sale_revenue"))
)

with_last = (
    with_latest_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.region",
        "s.order_channel",
        "s.sale_date",
        "s.store_sale_revenue",
        F.col("c.revenue"),
        F.col("c.sale_date").alias("store_sale_sale_date"),
    )
)

result = (
    with_last
    .groupBy(
        F.when(F.col("revenue") >= 3600, "LARGE").otherwise("SMALL").alias("revenue_tier"),
        F.date_trunc("year", "sale_date").alias("year_start"),
    )
    .agg(
        F.percentile_approx("revenue", [0.25, 0.5, 0.75]).alias("revenue_quartiles"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.avg("revenue").alias("avg_revenue"),
    )
    .filter(F.col("avg_revenue") < 1000000)
    .orderBy(F.col("year_start").desc())
)
