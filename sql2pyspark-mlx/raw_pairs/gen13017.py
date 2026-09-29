from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") > 222)
    .groupBy("product_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") < 7)
    .orderBy(F.col("cnt").desc())
)
