from pyspark.sql import functions as F

valid_inventory = (
    df_inventory
    .filter((F.col("last_restock") <= "2023-03-08") & ((F.col("last_restock") < "2025-04-01") | (F.col("store_id") == 2)))
)

agg = (
    valid_inventory
    .groupBy(F.year("last_restock").alias("last_restock_year"))
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = (
    agg
    .select("last_restock_year", "avg_quantity")
    .orderBy(F.col("last_restock_year").desc())
)
