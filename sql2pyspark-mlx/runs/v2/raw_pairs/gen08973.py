from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter((F.col("revenue") > 1000) & (F.col("units") <= 29))
    .withColumn("revenue_per_unit", (F.col("units") - F.col("revenue")))
)

agg = (
    base
    .groupBy("product_id", "store_id")
    .agg(
        F.avg("units").alias("avg_units"),
        F.sum("units").alias("total_units"),
    )
)

result = (
    agg
    .filter(F.col("total_units") > 230)
    .orderBy("store_id")
)
