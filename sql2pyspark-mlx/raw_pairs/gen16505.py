from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipped_date").desc(), F.col("c.id").desc())

latest_match = (
    df_orders
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.store_id",
        "s.amount",
        "s.discount",
        "s.shipping_fee",
        F.col("c.carrier").alias("shipment_carrier"),
    )
)

result = (
    latest_match
    .select(
        "id",
        "customer_id",
        "store_id",
        "amount",
        "discount",
        "shipping_fee",
        "shipment_carrier",
    )
)
