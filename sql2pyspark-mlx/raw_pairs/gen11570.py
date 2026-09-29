from pyspark.sql import functions as F

result = (
    df_inventory
    .agg(
        F.min("last_restock").alias("min_last_restock"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
    .orderBy(F.col("quantity_range").desc())
)
