from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") > 20)
    .select("id", "product_id")
    .orderBy(F.col("product_id").desc())
)
