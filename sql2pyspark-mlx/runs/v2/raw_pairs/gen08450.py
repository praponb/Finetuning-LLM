from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("id", "units", "product_id")
    .orderBy(F.col("units").desc(), "id")
    .limit(5)
)
