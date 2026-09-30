from pyspark.sql import functions as F

valid_store_sales = df_store_sales.filter(F.col("product_id").isin([2, 4, 5, 7]))

store_sales_stats = (
    valid_store_sales
    .groupBy(F.quarter("sale_date").alias("sale_quarter"))
    .agg(
        F.count("*").alias("cnt"),
        F.sum("units").alias("total_units"),
    )
)

result = (
    store_sales_stats
    .filter(F.col("total_units") > 180)
    .select("sale_quarter", "cnt")
    .orderBy("sale_quarter", F.col("cnt").desc())
)
