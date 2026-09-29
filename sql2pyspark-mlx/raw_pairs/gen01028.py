from pyspark.sql import functions as F

result = (
    df_products
    .filter(~((F.col("supplier_id") == 24) & (F.col("supplier_id") == 37)))
    .select("id", "launch_date", "tags")
    .orderBy(F.col("id").desc())
)
