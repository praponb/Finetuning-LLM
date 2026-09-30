from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(202, 222))
    .groupBy("store_id")
    .agg(
        F.count("*").alias("cnt"),
        F.min("quantity").alias("min_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .filter(F.col("avg_quantity") < 160)
    .drop("avg_quantity")
)
