from pyspark.sql import functions as F

result = (
    df_products
    .select("price")
    .unionByName(
        df_store_sales
        .filter(F.year("sale_date") == 2025)
        .select(F.col("revenue").alias("price"))
    )
    .distinct()
)
