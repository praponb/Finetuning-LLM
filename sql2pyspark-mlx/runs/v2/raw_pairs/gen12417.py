from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2]))
    .withColumn("last_restock_period", F.date_trunc("WEEK", "last_restock"))
    .withColumn("quantity_filled", F.coalesce("quantity", F.lit(0)))
    .orderBy(F.col("quantity").desc())
)
