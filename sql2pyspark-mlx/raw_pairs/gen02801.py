from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("month", "end_date"), "plan").orderBy("end_date", "id")
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.max("amount").alias("max_order_amount"))

scored = (
    df_subscriptions
    .withColumn("cumulative_monthly_fee", F.sum("monthly_fee").over(w))
    .withColumn("prev_monthly_fee", F.lag("monthly_fee", 2).over(w2))
)

result = (
    scored
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "monthly_fee",
        "status",
        "prev_monthly_fee",
        F.col("max_order_amount"),
    )
)
