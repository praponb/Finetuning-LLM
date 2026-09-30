from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(29, 38) & (F.col("units") > 37))
    .select("store_id", "product_id")
)
