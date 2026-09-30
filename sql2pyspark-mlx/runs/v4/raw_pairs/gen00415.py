from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id", "product_id")
    .agg(
        F.max("sale_date").alias("latest_sale_date"),
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.min("sale_date").alias("min_sale_date"),
    )
    .filter(F.col("avg_units") < 75)
    .orderBy("min_sale_date", F.col("store_id").desc())
)
