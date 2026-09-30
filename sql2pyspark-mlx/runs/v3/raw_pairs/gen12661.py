from pyspark.sql import functions as F

customer_totals = (
    df_orders
    .filter(F.col("customer_id").isin([4, 5, 8, 32]))
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("row_count"),
        F.sum("amount").alias("total_amount"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("C"), F.col("c.id") == F.col("C.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.segment"),
        F.col("C.row_count"),
        F.col("C.total_amount"),
    )
)
