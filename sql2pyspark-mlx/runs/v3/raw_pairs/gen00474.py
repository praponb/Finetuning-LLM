from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(2100, 2500))
    .groupBy(F.month("sale_date").alias("sale_month"), "store_id")
    .agg(
        F.countDistinct("revenue").alias("unique_stores"),
        F.max("sale_date").alias("latest_sale_date"),
    )
    .orderBy(F.col("latest_sale_date").desc())
)
