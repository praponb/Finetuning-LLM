from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity").between(80, 228) & (F.col("last_restock") >= "2024-03-19")) | F.col("quantity").between(113, 178))
    .select("quantity", F.date_add("last_restock", 84).alias("last_restock_plus_84d"))
)
