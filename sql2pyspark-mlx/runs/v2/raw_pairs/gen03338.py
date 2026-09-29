from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.avg("units").alias("avg_units"),
        F.max("sale_date").alias("latest_sale_date"),
    )
    .orderBy(F.col("product_id").desc())
    .limit(1)
)
