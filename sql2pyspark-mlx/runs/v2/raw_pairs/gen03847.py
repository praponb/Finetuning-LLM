from pyspark.sql import functions as F

customer_totals = (
    df_orders
    .filter(F.col("shipping_fee") >= 11)
    .groupBy("customer_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_returned"),
        F.sum("discount").alias("total_discount"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("t"), F.col("c.id") == F.col("t.customer_id"), "left")
    .filter(F.col("t.pct_returned") > 47)
    .select(F.col("c.credit_limit"), F.col("t.pct_returned"), F.col("t.total_discount"))
    .orderBy(F.col("credit_limit").desc(), "total_discount")
)
