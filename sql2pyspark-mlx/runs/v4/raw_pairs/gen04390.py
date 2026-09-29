from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id") == 10)
    .withColumn("quantity_filled", F.coalesce("quantity", F.lit(0)))
    .withColumn("last_restock_minus_72d", F.date_sub("last_restock", 72))
    .orderBy("id")
    .limit(10)
)
