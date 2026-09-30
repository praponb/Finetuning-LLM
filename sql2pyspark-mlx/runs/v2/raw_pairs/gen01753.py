from pyspark.sql import functions as F

valid_store_sales = df_store_sales.filter(F.col("store_id") == 1)

agg = (
    valid_store_sales
    .groupBy("store_id", F.quarter("sale_date").alias("sale_quarter"))
    .agg(
        F.min("sale_date").alias("first_sale_date"),
        F.count("*").alias("cnt"),
    )
)

result = agg.select("store_id", "sale_quarter", "first_sale_date", "cnt")
