from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
unit_price_by_order_id = df_order_items.groupBy("order_id").agg(F.min("unit_price").alias("min_order_item_unit_price"))

flagged = (
    df_payments
    .withColumn("new_group_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_payments = (
    flagged
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        F.sum("new_group_flag").over(w2).alias("island_id"),
    )
)

result = (
    sessions_payments
    .join(unit_price_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "island_id",
        F.when(F.col("method").rlike("^(paypal|promptpay)$"), "GROUP_A").when(F.col("method").rlike("^(card|bank_transfer)$"), "PRIORITY").otherwise("CORE").alias("method_zone"),
        F.col("min_order_item_unit_price"),
    )
)
