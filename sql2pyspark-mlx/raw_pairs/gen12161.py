from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
unit_price_by_order_id = df_order_items.groupBy("order_id").agg(F.min("unit_price").alias("min_order_item_unit_price"))

gaps = (
    df_payments
    .withColumn("is_new_session", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    sessions_payments
    .join(unit_price_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "status",
        "is_new_session",
        "session_no",
        F.col("min_order_item_unit_price"),
    )
    .orderBy(F.col("order_id").asc_nulls_last(), "is_new_session")
)
