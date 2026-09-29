from pyspark.sql import functions as F

result = (
    df_products
    .select("brand")
    .subtract(df_stores.select("store_name"))
    .orderBy(F.col("brand").desc())
)
