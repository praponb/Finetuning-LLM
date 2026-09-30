from pyspark.sql import functions as F

base = df_shipments.filter(F.col("order_id").isin([19, 36]))

summary = (
    base
    .groupBy("order_id")
    .agg(F.max("cost").alias("max_cost"))
)

result = (
    summary
    .filter(F.col("max_cost") < 82)
    .orderBy(F.col("order_id").desc())
    .limit(10)
)
