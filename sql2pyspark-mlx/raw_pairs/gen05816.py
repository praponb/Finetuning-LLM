from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .agg(F.max("launch_date").alias("last_launch_date"))
)
