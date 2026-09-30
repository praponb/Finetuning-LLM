from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

base = (
    df_orders
    .filter(F.col("channel").isin(["mobile", "partner"]))
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        F.greatest("discount", "shipping_fee").alias("greatest_discount_shipping_fee"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_filled"),
    )
)

result = (
    base
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price").alias("order_item_unit_price"))
)
