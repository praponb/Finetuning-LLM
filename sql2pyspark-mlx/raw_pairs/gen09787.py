from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.delivered_date").desc(), F.col("c.id").desc())

filtered = (
    df_orders
    .filter(F.col("order_date") >= F.lit("2023-03-25").cast("date"))
    .select(
        "*",
        F.datediff(F.current_date(), "order_date").alias("days_since_order_date"),
        (F.col("amount") - F.col("shipping_fee")).alias("amount_with_shipping"),
        F.coalesce("channel", F.lit("N/A")).alias("resolved_channel"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.status").alias("shipment_status"),
        F.col("c.carrier").alias("shipment_carrier"),
    )
)
