from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter((F.col("units") < 33) & (F.col("units").between(34, 37) | F.col("product_id").isin([6, 8])))
)

agg = (
    base
    .groupBy("product_id")
    .agg(F.round(F.avg("revenue"), 2).alias("avg_revenue"))
)

result = (
    agg
    .orderBy(F.col("product_id").desc())
    .limit(1)
)
