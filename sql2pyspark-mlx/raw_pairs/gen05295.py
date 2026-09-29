from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(14, 28))
    .agg(
        F.count("*").alias("num_store_sales"),
        F.min("sale_date").alias("min_sale_date"),
    )
)
