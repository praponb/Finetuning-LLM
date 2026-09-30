from pyspark.sql import functions as F

result = (
    df_inventory
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("avg_quantity").desc(), F.col("cnt").desc())
)
