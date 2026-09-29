from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") >= 162)
    .withColumn("last_restock_weekofyear", F.weekofyear("last_restock"))
)
