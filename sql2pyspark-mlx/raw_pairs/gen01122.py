from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") < "2024-07-01")
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") > 4)
    .select("product_id", "stddev_quantity", "cnt")
    .orderBy(F.col("product_id").desc())
    .limit(50)
)
