from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

filtered = (
    df_orders
    .filter(F.col("order_date") > F.lit("2023-03-21").cast("date"))
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.trim("channel").alias("clean_channel"),
    )
)

eligible = (
    filtered
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") >= "2024-06-01"), "left_semi")
)

with_last = (
    eligible
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.paid_at"))
)

kept = (
    with_last
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.quantity") <= 5), "left_semi")
)

result = (
    kept
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        "clean_channel",
        F.when(F.col("channel").rlike("^(web|mobile)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("channel_zone"),
    )
)
