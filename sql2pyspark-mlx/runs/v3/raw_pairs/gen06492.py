from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") == 209) | (F.col("quantity") != 220))
    .withColumn("last_restock_period", F.date_trunc("week", "last_restock"))
)
