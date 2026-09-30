from pyspark.sql import functions as F

result = (
    df_inventory
    .select("quantity", "store_id", F.year("last_restock").alias("last_restock_year"))
)
