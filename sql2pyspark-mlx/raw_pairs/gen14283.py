from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

with_last = (
    df_orders
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.status",
        "s.channel",
        "s.amount",
        "s.discount",
        F.col("c.amount").alias("payment_amount"),
    )
)

result = (
    with_last
    .select(
        "id",
        "status",
        "channel",
        "discount",
        F.when(F.col("channel").rlike("^(partner|store)$"), "GROUP_B").when(F.col("channel").rlike("^(web|mobile)$"), "PRIORITY").otherwise("GROUP_A").alias("channel_zone"),
    )
)
