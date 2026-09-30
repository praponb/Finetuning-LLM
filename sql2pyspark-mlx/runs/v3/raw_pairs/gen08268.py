from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id", F.year("sale_date").alias("sale_year"))
    .agg(
        F.count("revenue").alias("revenue_count"),
        F.countDistinct("revenue").alias("unique_stores"),
        F.max("sale_date").alias("latest_sale_date"),
    )
)
