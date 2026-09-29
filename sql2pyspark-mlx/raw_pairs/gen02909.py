from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("order_id").orderBy("cost", "id")
w_ordered = Window.partitionBy(F.month("shipped_date"), "status").orderBy("shipped_date", "id")
w_running = Window.partitionBy("status", "carrier").orderBy("cost", "id")
amount_by_order_id = df_payments.groupBy("order_id").agg(F.max("amount").alias("max_payment_amount"))

with_windows = (
    df_shipments
    .filter(F.col("order_id").isin([10, 40]))
    .withColumn("prev_cost", F.lag("cost", 2, 0).over(w_grp))
    .withColumn("prev_cost_2", F.lag("cost", 1).over(w_ordered))
    .withColumn("cost_bucket", F.ntile(4).over(w_running))
)

result = (
    with_windows
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "prev_cost",
        "prev_cost_2",
        "cost_bucket",
        F.when(F.col("status").rlike("^(LOST|DELIVERED)$"), "CORE").when(F.col("status").rlike("^(IN_TRANSIT|RETURNED)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("status_zone"),
        F.col("max_payment_amount"),
    )
    .orderBy(F.col("cost_bucket").asc_nulls_first())
)
