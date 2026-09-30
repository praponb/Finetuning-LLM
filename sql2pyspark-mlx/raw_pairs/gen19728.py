from pyspark.sql import functions as F

valid_inventory = (
    df_inventory
    .filter((F.col("store_id") == 3) & (F.col("last_restock") <= "2023-05-01"))
    .withColumn("last_restock_period", F.date_trunc("MONTH", "last_restock"))
)

summary = (
    valid_inventory
    .groupBy(F.quarter("last_restock_period").alias("last_restock_period_quarter"))
    .agg(F.max("quantity").alias("max_quantity"))
)

result = (
    summary
    .filter(F.col("max_quantity") > 230)
    .select("last_restock_period_quarter", "max_quantity")
    .orderBy(F.col("last_restock_period_quarter").desc())
)
