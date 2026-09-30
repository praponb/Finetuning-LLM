from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("product_id", "sale_date")
    .orderBy(F.col("sale_date").desc())
)
