from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("supplier_id", "brand")
    .agg(
        F.max("launch_date").alias("latest_launch_date"),
        F.max("price").alias("max_price"),
    )
    .orderBy("brand")
)
