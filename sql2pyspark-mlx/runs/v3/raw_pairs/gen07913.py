from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") > "2023-03-02")
    .select("id", "product_id")
    .orderBy(F.col("product_id").desc(), F.col("id").desc())
)
