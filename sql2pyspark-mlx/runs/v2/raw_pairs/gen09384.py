from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([4, 5, 32]))
    .select("tags")
)
