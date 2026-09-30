from pyspark.sql import functions as F

src = (
    df_inventory
    .filter((F.col("quantity") == 226) & (F.col("quantity") < 107))
    .withColumn("last_restock_month", F.month("last_restock"))
)

agg = (
    src
    .groupBy("product_id")
    .agg(F.max("quantity").alias("max_quantity"))
)

result = (
    agg
    .filter(F.col("max_quantity") >= 220)
    .select("product_id", "max_quantity")
    .orderBy(F.col("max_quantity").desc())
)
