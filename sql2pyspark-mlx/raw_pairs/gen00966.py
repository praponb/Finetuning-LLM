from pyspark.sql import functions as F

customer_summary = (
    df_orders
    .filter(F.col("shipping_fee") <= 27)
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("status") == "SHIPPED", F.col("amount")).otherwise(0)).alias("shipped_amount"),
        F.round(((100 * F.sum(F.when(F.col("channel") == "store", 1).otherwise(0))) / F.count("*")), 1).alias("pct_store"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .filter(F.col("cs.shipped_amount") > 55)
    .select(
        F.col("c.id"),
        F.col("c.signup_date"),
        F.col("cs.shipped_amount"),
        F.col("cs.pct_store"),
    )
)
