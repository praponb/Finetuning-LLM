from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())

with_last = (
    df_customers
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.signup_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.channel").alias("order_channel"),
        F.col("c.shipping_fee").alias("order_shipping_fee"),
    )
)

result = (
    with_last
    .select(
        "id",
        "name",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "order_channel",
    )
)
