from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("product_name").like("%Run%") & (F.col("stock_qty") >= 322)) | (F.col("category") != "Fashion"))
    .groupBy("category")
    .agg(F.min("launch_date").alias("min_launch_date"))
    .orderBy(F.col("min_launch_date").desc())
)
