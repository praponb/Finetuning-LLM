from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("product_id", "units", F.col("units").cast("string").alias("units_str"))
    .orderBy(F.col("product_id").desc())
)
