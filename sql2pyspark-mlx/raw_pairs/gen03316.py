from pyspark.sql import functions as F

customer_stats = (
    df_orders
    .filter(F.col("channel").isin(["store", "web"]))
    .groupBy("customer_id")
    .agg(
        (F.max("discount") - F.min("discount")).alias("discount_range"),
        F.countDistinct("shipping_fee").alias("unique_customers"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .filter(F.col("cs.discount_range") >= 30)
    .select(
        F.col("c.id"),
        F.col("c.credit_limit"),
        F.col("cs.discount_range"),
        F.col("cs.unique_customers"),
    )
    .orderBy("id")
)
