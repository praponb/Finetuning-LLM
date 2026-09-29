from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date").isNotNull())
    .select("launch_date")
    .unionByName(
        df_shipments
        .filter(F.col("shipped_date").isNotNull())
        .select(F.col("shipped_date").alias("launch_date"))
    )
    .distinct()
    .orderBy(F.col("launch_date").desc())
)
