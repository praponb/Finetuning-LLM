from pyspark.sql import functions as F

result = (
    df_inventory
    .withColumn("quantity_num", F.col("quantity").cast("double"))
    .withColumn("last_restock_period", F.date_trunc("YEAR", "last_restock"))
    .orderBy("quantity", "id")
)
