from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "COMPLETED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_completed"),
        F.max("amount").alias("max_amount"),
    )
    .filter(F.col("pct_completed") >= 74)
    .select("order_month", "pct_completed", "max_amount")
)
