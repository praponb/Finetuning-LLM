from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id", "method").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist_2 = Window.orderBy("paid_at", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy("order_id")
w6 = Window.partitionBy("order_id", "method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w7 = Window.orderBy(F.col("amount").desc(), "id")
w8 = Window.partitionBy("order_id").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("channel").orderBy("order_id_min_amount", "id")

ranked = (
    df_payments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

metrics = (
    ranked
    .withColumn("last_known_method", F.last("method", ignorenulls=True).over(w2))
    .withColumn("running_amount", F.sum("amount").over(w_hist))
    .withColumn("amount_avg_6_prior", F.avg("amount").over(w_hist_2))
    .withColumn("order_id_min_amount", F.min("amount").over(w5))
    .withColumn("amount_count_30d", F.count("amount").over(w6))
)

with_dims = (
    metrics
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.shipping_fee"), F.col("b.channel"), F.col("b.discount"))
)

scored = (
    with_dims
    .withColumn("seq_no", F.row_number().over(w7))
    .withColumn("last_known_order_id_min_amount", F.last("order_id_min_amount", ignorenulls=True).over(w8))
    .withColumn("next_running_amount", F.lead("running_amount", 1).over(w9))
)

result = (
    scored
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        "last_known_method",
        "running_amount",
        "amount_avg_6_prior",
        "channel",
        F.when(F.col("channel").rlike("^(partner|mobile)$"), "GROUP_B").when(F.col("channel").rlike("^(web|store)$"), "PRIORITY").otherwise("EXTENDED").alias("channel_zone"),
    )
    .orderBy(F.col("channel_zone").desc())
)
