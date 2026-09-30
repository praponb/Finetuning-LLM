from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("region", "city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city", "region").orderBy(F.col("sqft").desc(), "id")
w4 = Window.partitionBy("region")

filtered = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-04-28").cast("date")) & (F.length("store_name") > 7))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        F.add_months("opened_date", 3).alias("opened_date_plus_months"),
        F.coalesce("region", F.lit("OTHER")).alias("region_filled"),
    )
)

kept = (
    filtered
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.last_restock") >= F.col("s.opened_date_plus_months")), "left_anti")
)

with_latest = (
    kept
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.order_date"))
)

result = (
    with_latest
    .select(
        "*",
        F.last("region", ignorenulls=True).over(w2).alias("last_known_region"),
        F.ntile(5).over(w3).alias("sqft_bucket"),
        F.sum("sqft").over(w4).alias("region_sum_sqft"),
    )
)
