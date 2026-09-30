from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("id", "sale_date", "product_id", "units")
    .orderBy(F.col("units").desc(), "product_id")
)
