from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 5]) & (F.col("product_id").isin([4, 7]) | (F.col("product_id") == 3)))
    .groupBy(F.quarter("sale_date").alias("sale_quarter"))
    .agg(
        F.min("sale_date").alias("first_sale_date"),
        (F.max("units") - F.min("units")).alias("units_range"),
        F.count("units").alias("units_count"),
    )
    .orderBy("units_count")
)
