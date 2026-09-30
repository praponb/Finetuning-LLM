from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") < 29) & (F.col("units") <= 19))
    .groupBy("store_id")
    .agg(
        F.sum("units").alias("total_units"),
        F.countDistinct("units").alias("unique_products"),
        F.max("sale_date").alias("last_sale_date"),
    )
    .filter(F.col("unique_products") < 1)
    .orderBy("unique_products")
)
