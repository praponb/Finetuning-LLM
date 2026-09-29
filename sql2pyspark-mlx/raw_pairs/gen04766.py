from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")
w_recent = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.date_trunc("YEAR", "opened_date"), "region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

base = (
    df_stores
    .filter(F.col("store_name").like("%Downtown%"))
    .withColumn("clean_store_name", F.trim("store_name"))
)

enriched = (
    base
    .withColumn("sqft_bucket", F.ntile(3).over(w))
    .withColumn("cumulative_sqft", F.sum("sqft").over(w_recent))
    .withColumn("running_sqft", F.sum("sqft").over(w3))
)

eligible = (
    enriched
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.month(F.col("c.sale_date")).isin([2, 7, 8])), "left_semi")
)

with_latest = (
    eligible
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"))
)

not_flagged = (
    with_latest
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.store_id") == 40), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "clean_store_name",
        "cumulative_sqft",
        "running_sqft",
    )
    .orderBy(F.col("cumulative_sqft").desc_nulls_first())
)
