from pyspark.sql import functions as F

result = (
    df_inventory
    .select("id", "product_id")
    .orderBy("id", F.col("product_id").desc())
)
