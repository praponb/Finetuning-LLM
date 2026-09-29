from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
unit_price_by_order_id = df_order_items.groupBy("order_id").agg(F.max("unit_price").alias("max_order_item_unit_price"))

enriched = (
    df_payments
    .withColumn("last_known_amount", F.last("amount", ignorenulls=True).over(w))
    .withColumn("amount_avg_3", F.avg("amount").over(w2))
    .withColumn("first_known_method", F.first("method", ignorenulls=True).over(w3))
)

result = (
    enriched
    .join(unit_price_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "amount_avg_3",
        F.when(F.col("method").rlike("^(bank_transfer|cash)$"), "OTHER_ZONE").when(F.col("method").rlike("^(card|paypal)$"), "GROUP_A").otherwise("PRIORITY").alias("method_zone"),
        F.col("max_order_item_unit_price"),
    )
)
