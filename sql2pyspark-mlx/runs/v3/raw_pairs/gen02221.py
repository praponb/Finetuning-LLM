from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("s.cost").between(44, 63) | F.col("s.shipped_date").between("2023-07-05", "2024-07-12"))
    .select(
        F.col("s.order_id"),
        F.col("s.cost"),
        F.col("o.customer_id"),
        F.col("o.channel"),
        F.col("o.amount"),
        F.coalesce(F.col("s.cost"), F.lit(0)).alias("cost_filled"),
    )
    .orderBy(F.col("amount").desc(), "customer_id")
)
