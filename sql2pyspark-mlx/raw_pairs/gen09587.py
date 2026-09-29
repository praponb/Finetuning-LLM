from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date").between("2024-04-01", "2025-09-01"))
    .select(F.col("launch_date").alias("event_date"), F.lit("product").alias("kind"))
    .unionByName(
        df_orders
        .select(F.col("order_date").alias("event_date"), F.lit("order").alias("kind"))
    )
)
