from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") < 40)
    .select(F.lit("store_sale").alias("kind"), "sale_date")
    .union(df_inventory.select(F.lit("inventory").alias("kind"), "last_restock"))
    .orderBy(F.col("kind").desc())
)
