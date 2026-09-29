from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id") == 6)
    .groupBy("store_id", "product_id")
    .agg(
        F.min("sale_date").alias("first_sale_date"),
        F.max("units").alias("max_units"),
    )
    .orderBy(F.col("first_sale_date").desc())
)
