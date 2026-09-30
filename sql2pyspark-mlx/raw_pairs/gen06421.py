from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "method").orderBy(F.col("order_amount").desc(), "id")
w2 = Window.partitionBy("channel", "order_id").orderBy("rn", "id")

filtered = (
    df_payments
    .filter(F.col("method").isNotNull())
    .select(
        "*",
        F.hour("paid_at").alias("paid_at_hour"),
        F.initcap(F.trim("status")).alias("status_title"),
    )
)

enriched = (
    filtered
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.method",
        "src.amount",
        "src.paid_at_hour",
        "src.status_title",
        F.col("d.channel"),
        F.col("d.amount").alias("order_amount"),
    )
)

latest = (
    enriched
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

latest_payments = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest_payments
    .alias("a")
    .join(df_orders.alias("dim"), F.col("a.order_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["order_channel", "payment_channel"], ["order_channel"], []],
        F.col("dim.channel").alias("order_channel"),
        F.col("a.channel").alias("payment_channel"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("a.amount")), 2).alias("total_amount"),
        F.grouping_id().alias("gid"),
    )
)
