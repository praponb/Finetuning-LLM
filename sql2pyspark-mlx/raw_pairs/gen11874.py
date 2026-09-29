from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("s.delivered_date").between("2024-04-01", "2024-07-01"))
    .select(
        F.col("s.cost"),
        F.col("o.order_date"),
        F.col("o.discount"),
        F.col("o.customer_id"),
    )
    .orderBy(F.col("cost").desc())
)
