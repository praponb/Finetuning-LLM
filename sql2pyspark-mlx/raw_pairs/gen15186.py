from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

filtered = (
    df_stores
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-07-21"))) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "opened_date",
        "sqft",
        F.concat_ws("-", "store_name", "region").alias("store_name_region_key"),
        F.upper("region").alias("region_upper"),
    )
)

with_latest = (
    filtered
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.store_name", F.col("c.revenue"), F.col("c.units"))
)

latest_match = (
    with_latest
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.store_name", F.col("c.last_restock"), F.col("c.quantity"))
)

base = (
    latest_match
    .filter(F.col("quantity") <= 140)
    .withColumn("store_name_length", F.length("store_name"))
)

kept = (
    base
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.order_date") >= F.col("s.last_restock")), "left_semi")
)

grouped = (
    kept
    .groupBy(
        F.date_trunc("week", "last_restock").alias("week_start"),
        F.when(F.col("quantity") >= 219, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.percentile_approx("store_name_length", 0.9).alias("store_name_length_p90"),
    )
)

result = (
    grouped
    .select("week_start", "quantity_tier", "store_name_length_p90")
    .orderBy(F.col("quantity_tier").desc())
)
