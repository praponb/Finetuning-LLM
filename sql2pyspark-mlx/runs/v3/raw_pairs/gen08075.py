from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([3, 4]))
    .groupBy("store_id", F.quarter("sale_date").alias("sale_quarter"))
    .agg(
        F.count("*").alias("store_sale_count"),
        F.sum("units").alias("total_units"),
    )
)
