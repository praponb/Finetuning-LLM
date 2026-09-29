from pyspark.sql import functions as F

recent_store_sales = df_store_sales.filter(F.col("units") != 38)

summary = (
    recent_store_sales
    .groupBy("product_id")
    .agg(
        F.count("*").alias("num_store_sales"),
        F.max("units").alias("max_units"),
    )
)

result = summary.select("product_id", "num_store_sales", "max_units")
