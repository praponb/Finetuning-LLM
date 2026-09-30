from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([6, 9]))
    .groupBy("product_id")
    .agg(
        F.max("sale_date").alias("last_sale_date"),
        F.min("sale_date").alias("first_sale_date"),
        F.count("*").alias("num_store_sales"),
    )
)
