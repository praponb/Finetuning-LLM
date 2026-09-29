from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") >= "2024-05-22")
    .select("quantity")
)
