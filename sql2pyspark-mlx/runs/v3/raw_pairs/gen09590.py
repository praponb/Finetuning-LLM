from pyspark.sql import functions as F

result = (
    df_inventory
    .withColumn("last_restock_plus_months", F.add_months("last_restock", 3))
    .withColumn("quantity_whole", F.floor("quantity"))
)
