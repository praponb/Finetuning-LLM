from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .groupBy(
        F.when(F.year(F.col("stores.opened_date")) < 2024, "before_2024").when(F.year(F.col("stores.opened_date")) == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(
        (F.max(F.col("orders.shipping_fee")) - F.min(F.col("orders.shipping_fee"))).alias("shipping_fee_range"),
        F.sum(F.when(F.col("orders.status") == "SHIPPED", F.col("orders.amount")).otherwise(0)).alias("shipped_amount"),
    )
    .orderBy(F.col("shipping_fee_range").desc(), "opened_cohort")
    .limit(10)
)
