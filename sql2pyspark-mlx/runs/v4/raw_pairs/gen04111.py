from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") < "2023-03-21")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.max("quantity").alias("max_quantity"),
        F.count("*").alias("num_inventory"),
    )
    .orderBy("quantity_range")
)
