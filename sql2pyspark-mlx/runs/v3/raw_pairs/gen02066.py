from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 7, 8, 10]))
    .groupBy("store_id")
    .agg(
        F.max("sale_date").alias("latest_sale_date"),
        F.min("sale_date").alias("first_sale_date"),
    )
    .orderBy(F.col("latest_sale_date").desc())
)
