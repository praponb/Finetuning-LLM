from pyspark.sql import functions as F

customer_totals = (
    df_orders
    .filter(F.col("amount").isNotNull())
    .groupBy("customer_id")
    .agg(
        F.countDistinct("shipping_fee").alias("unique_stores"),
        F.sum(F.when(F.col("channel") == "mobile", F.col("amount")).otherwise(0)).alias("mobile_amount"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "left")
    .filter(F.col("ct.mobile_amount") > 24)
    .select(
        F.col("c.id"),
        F.col("c.city"),
        F.col("ct.unique_stores"),
        F.col("ct.mobile_amount"),
    )
    .orderBy("city")
)
