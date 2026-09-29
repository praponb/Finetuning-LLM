from pyspark.sql import functions as F

customer_totals = (
    df_orders
    .groupBy("customer_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("channel") == "store", 1).otherwise(0))) / F.count("*")), 1).alias("pct_store"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "inner")
    .filter(F.col("ct.pct_store") > 78)
    .select(F.col("c.id"), F.col("c.email"), F.col("ct.pct_store"))
    .orderBy(F.col("pct_store").desc())
)
