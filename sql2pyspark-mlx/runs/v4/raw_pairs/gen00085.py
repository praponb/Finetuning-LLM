from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "method").orderBy(F.col("paid_at").asc_nulls_first())

valid = (
    df_payments
    .filter(F.col("order_id").isNotNull() & F.col("order_id").isNull())
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.floor("amount").alias("amount_whole"),
    )
)

ranked = (
    valid
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched = (
    ranked
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.discount"), F.col("b.shipping_fee"))
)

result = (
    enriched
    .select("id", "method", "shipping_fee", "discount")
    .unpivot(["id", "method"], ["shipping_fee", "discount"], "measure", "value")
)
