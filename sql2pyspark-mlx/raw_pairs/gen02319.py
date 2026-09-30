from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") > 183)
    .withColumn("quantity_num", F.col("quantity").cast("string"))
    .withColumn("quantity_filled", F.coalesce("quantity", F.lit(0)))
    .orderBy(F.col("last_restock").desc())
)
