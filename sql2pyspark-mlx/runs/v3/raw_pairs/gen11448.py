from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") < "2023-12-27")
    .groupBy("product_id")
    .agg(
        F.countDistinct("revenue").alias("unique_stores"),
        F.max("sale_date").alias("latest_sale_date"),
    )
    .orderBy("unique_stores", F.col("latest_sale_date").desc())
)
