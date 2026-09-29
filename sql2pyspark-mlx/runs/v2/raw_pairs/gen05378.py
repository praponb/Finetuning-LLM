from pyspark.sql import functions as F

base = (
    df_orders
    .filter(F.col("store_id").isin([2, 25, 31, 32]) & (F.col("order_date") > "2024-10-01"))
)

order_month_summary = (
    base
    .groupBy(F.date_trunc("MONTH", "order_date").alias("order_month"), "customer_id")
    .agg(
        F.min("discount").alias("min_discount"),
        F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0)).alias("returned_count"),
    )
)

result = (
    order_month_summary
    .orderBy(F.col("returned_count").desc(), F.col("customer_id").desc())
)
