from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "bestseller"))
    .select("launch_date")
    .unionByName(
        df_shipments
        .filter(F.col("status") == "DELIVERED")
        .select(F.col("delivered_date").alias("launch_date"))
    )
    .distinct()
)
