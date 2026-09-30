from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.year("sale_date").alias("sale_year"), "store_id")
    .agg(
        F.countDistinct("revenue").alias("unique_products"),
        F.min("sale_date").alias("first_sale_date"),
    )
    .orderBy(F.col("store_id").desc())
)
