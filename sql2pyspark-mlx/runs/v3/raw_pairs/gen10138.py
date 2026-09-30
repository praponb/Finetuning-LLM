from pyspark.sql import functions as F

result = (
    df_store_sales
    .cube("store_id", F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(F.countDistinct("units").alias("unique_products"))
    .orderBy(F.col("sale_month").desc())
)
