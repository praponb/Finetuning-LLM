from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

cleaned = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-10-09").cast("date")) & (F.col("channel").eqNullSafe(F.lit("partner"))))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "shipping_fee",
        F.date_trunc("month", "order_date").alias("order_date_period"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_filled"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_id",
        "s.status",
        "s.channel",
        "s.shipping_fee",
        F.col("c.paid_at"),
    )
)
