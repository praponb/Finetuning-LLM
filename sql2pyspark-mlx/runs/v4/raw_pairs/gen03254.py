from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") <= "2024-11-01")
    .select("id", "cost", "supplier_id", "tags")
    .orderBy(F.col("cost").desc(), F.col("supplier_id").desc())
)
