from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") <= 11) & (F.col("units") == 30) & F.col("product_id").isin([1, 9]))
    .groupBy("store_id")
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.count("*").alias("total_count"),
        F.min("sale_date").alias("first_sale_date"),
    )
)
