from pyspark.sql import functions as F

valid_products = (
    df_products
    .filter((F.col("product_name").like("Y%") & (F.col("stock_qty") == 142)) | (F.col("stock_qty") < 181))
    .withColumn("product_name_length", F.length("product_name"))
)

brand_summary = (
    valid_products
    .groupBy("brand", "category")
    .agg(
        F.max("cost").alias("max_cost"),
        F.avg("stock_qty").alias("avg_stock_qty"),
    )
)

result = (
    brand_summary
    .filter(F.col("avg_stock_qty") >= 380)
    .orderBy(F.col("max_cost").desc())
)
