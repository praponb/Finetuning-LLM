from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") > "2023-07-01")
    .groupBy("supplier_id")
    .agg(
        F.count(F.when(F.col("brand") == "Initech", 1)).alias("initech_count"),
        F.min("stock_qty").alias("min_stock_qty"),
        F.count(F.when(F.col("brand") == "Stark", 1)).alias("stark_count"),
    )
    .orderBy(F.col("initech_count").desc())
)
