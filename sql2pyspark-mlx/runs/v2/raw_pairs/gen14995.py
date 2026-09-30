from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(~((F.col("units") == 32) | F.col("store_id").isin([3, 4, 5, 6])))
    .groupBy("product_id")
    .agg(
        F.count("units").alias("units_count"),
        F.max("sale_date").alias("latest_sale_date"),
        F.min("sale_date").alias("first_sale_date"),
    )
    .orderBy("latest_sale_date", F.col("product_id").desc())
)
