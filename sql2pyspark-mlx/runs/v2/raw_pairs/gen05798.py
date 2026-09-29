from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("c.email").like("%@outlook.com"))
    .groupBy(F.col("c.id"), F.col("c.country"))
    .agg(
        F.count(F.col("o.id")).alias("order_count"),
        F.count(F.when(F.col("o.status") == "CANCELLED", 1)).alias("cancelled_count"),
    )
    .filter(F.col("order_count") > 2)
    .orderBy(F.col("id").desc(), "country")
    .limit(3)
)
