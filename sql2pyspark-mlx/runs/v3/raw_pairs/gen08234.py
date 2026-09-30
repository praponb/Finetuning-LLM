from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter((F.year(F.col("s.shipped_date")) == 2024) | F.col("s.shipped_date").between("2023-11-01", "2025-04-30"))
    .select(
        F.col("s.cost"),
        F.col("s.carrier"),
        F.col("s.delivered_date"),
        F.col("o.channel"),
    )
    .orderBy(F.col("delivered_date").desc())
)
