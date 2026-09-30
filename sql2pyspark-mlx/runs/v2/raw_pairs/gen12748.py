from pyspark.sql import functions as F

customer_totals = (
    df_orders
    .filter(F.col("customer_id").isNull())
    .groupBy("customer_id")
    .agg(F.min("order_date").alias("first_order_date"))
)

result = (
    df_customers.alias("customers")
    .join(customer_totals.alias("customer_totals"), F.col("customers.id") == F.col("customer_totals.customer_id"), "inner")
    .select(
        F.col("customers.age"),
        F.col("customers.name"),
        F.col("customer_totals.first_order_date"),
    )
    .orderBy("name")
)
