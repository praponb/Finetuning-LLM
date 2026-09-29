from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.date_trunc("MONTH", "last_restock").alias("last_restock_month"))
    .agg(F.sum("quantity").alias("total_quantity"))
    .filter(F.col("total_quantity") < 1090)
    .orderBy(F.col("total_quantity").desc())
)
