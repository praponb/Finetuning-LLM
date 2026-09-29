from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id", "product_id")
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.min("sale_date").alias("min_sale_date"),
    )
    .filter(F.col("row_count") >= 5)
    .orderBy(F.col("avg_units").desc(), "store_id")
)
