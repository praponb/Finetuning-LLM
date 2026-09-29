from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())

base = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "channel",
        "shipping_fee",
        (F.col("amount") - F.col("shipping_fee")).alias("amount_with_shipping"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("resolved_channel"),
    )
)

result = (
    base
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.status",
        "s.amount_with_shipping",
        F.col("c.quantity"),
        F.col("c.unit_price").alias("order_item_unit_price"),
    )
)
