from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(3400, 3900))
    .groupBy("store_id", "product_id")
    .agg(
        F.count("*").alias("total_count"),
        F.avg("units").alias("avg_units"),
        F.max("sale_date").alias("last_sale_date"),
    )
    .filter(F.col("avg_units") >= 33)
)
