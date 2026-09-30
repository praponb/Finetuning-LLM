from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").isNotNull() | (F.col("quantity") > 189))
    .withColumn("last_restock_year", F.year("last_restock"))
)
