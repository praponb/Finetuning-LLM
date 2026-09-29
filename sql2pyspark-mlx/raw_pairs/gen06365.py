from pyspark.sql import functions as F

kept = (
    df_orders
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") < "2024-12-01"), "left_anti")
)

result = (
    kept
    .withColumn("quarter_start", F.date_trunc("quarter", "order_date"))
    .groupingSets(
        [["customer_id", "status", "quarter_start"], ["customer_id", "status"], []],
        "customer_id",
        "status",
        "quarter_start",
    )
    .agg(
        F.avg(F.when(F.col("status") == "COMPLETED", F.col("discount"))).alias("completed_avg_discount"),
        F.count("*").alias("event_count"),
        F.corr("discount", "shipping_fee").alias("corr_discount_shipping_fee"),
        F.grouping("customer_id").alias("gid"),
    )
    .filter(F.col("event_count") > 1)
)
