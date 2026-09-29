from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_order_id = df_payments.groupBy("order_id").agg(F.avg("amount").alias("avg_payment_amount"))

metrics = (
    df_shipments
    .select(
        "*",
        F.last("cost", ignorenulls=True).over(w).alias("last_known_cost"),
        F.sum("cost").over(w2).alias("running_cost"),
    )
)

result = (
    metrics
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "delivered_date",
        "status",
        "running_cost",
        F.col("avg_payment_amount"),
    )
    .orderBy(F.col("avg_payment_amount").desc())
)
