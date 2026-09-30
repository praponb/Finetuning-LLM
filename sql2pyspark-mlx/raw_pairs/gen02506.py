from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("o.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.city"))
    .agg(
        F.count(F.col("o.id")).alias("order_count"),
        F.round(((100 * F.sum(F.when(F.col("o.status") == "RETURNED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_returned"),
        F.countDistinct(F.col("o.amount")).alias("unique_stores"),
    )
    .filter(F.col("order_count") >= 1)
    .orderBy(F.col("city").desc(), F.col("unique_stores").desc())
)
