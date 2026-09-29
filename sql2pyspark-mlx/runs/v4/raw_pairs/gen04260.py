from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft", "id")
w2 = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")
w3 = Window.partitionBy("region").orderBy(F.col("opened_date").desc(), "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())

scored = (
    df_stores
    .select(
        "*",
        F.ntile(5).over(w).alias("sqft_bucket"),
        F.ntile(3).over(w2).alias("sqft_bucket_2"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

latest_match = (
    scored
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.discount"))
)

kept = (
    latest_match
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.order_date") >= F.col("s.opened_date")), "left_semi")
)

latest_match_stores = (
    kept
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.shipping_fee").alias("order_shipping_fee"))
)

result = (
    latest_match_stores
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft_bucket",
        "event_seq",
        "discount",
        "order_shipping_fee",
    )
    .orderBy(F.col("id").desc_nulls_first(), "sqft_bucket")
)
