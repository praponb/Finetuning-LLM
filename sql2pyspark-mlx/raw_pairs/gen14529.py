from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w3 = Window.orderBy("opened_date", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("region").orderBy("shipping_fee", "id")
w5 = Window.partitionBy(F.year("opened_date")).orderBy("shipping_fee", "id")
w6 = Window.partitionBy(F.date_trunc("YEAR", "opened_date"), "city").orderBy(F.col("shipping_fee").desc())

valid = (
    df_stores
    .filter((~F.col("region").eqNullSafe(F.lit("East"))) & (F.col("region") == "South"))
    .withColumn("city_prefix", F.substring("city", 1, 2))
)

with_last = (
    valid
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.shipping_fee"))
)

with_last_stores = (
    with_last
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date"))
)

metrics = (
    with_last_stores
    .withColumn("sqft_avg_6", F.avg("sqft").over(w3))
    .withColumn("prev_shipping_fee", F.lag("shipping_fee", 2).over(w4))
    .withColumn("shipping_fee_bucket", F.ntile(5).over(w5))
    .withColumn("shipping_fee_dense_rank", F.dense_rank().over(w6))
)

result = (
    metrics
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        "sqft",
        "city_prefix",
        "shipping_fee",
        "sale_date",
        "sqft_avg_6",
    )
)
