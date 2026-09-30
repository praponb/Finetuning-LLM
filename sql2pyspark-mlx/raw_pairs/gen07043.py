from pyspark.sql import functions as F

result = (
    df_products
    .select("launch_date")
    .unionByName(
        df_stores
        .filter(F.col("store_name").isNotNull())
        .select(F.col("opened_date").alias("launch_date"))
    )
)
