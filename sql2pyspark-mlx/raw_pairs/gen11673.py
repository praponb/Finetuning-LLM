from pyspark.sql import functions as F

customer_totals = (
    df_orders
    .filter(F.year("order_date") >= 2025)
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("channel") == "partner", 1).otherwise(0)).alias("partner_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("C"), F.col("c.id") == F.col("C.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.segment"),
        F.col("c.city"),
        F.col("C.partner_count"),
    )
    .orderBy(F.col("id").desc())
)
