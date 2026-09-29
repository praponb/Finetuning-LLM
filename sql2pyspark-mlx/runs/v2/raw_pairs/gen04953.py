from pyspark.sql import functions as F

result = (
    df_inventory
    .select("id", "last_restock", F.month("last_restock").alias("last_restock_month"))
)
