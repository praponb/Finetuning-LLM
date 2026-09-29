from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([18, 22, 29, 31]))
    .groupBy("order_id")
    .agg(F.sum("cost").alias("total_cost"))
    .orderBy(F.col("total_cost").desc())
)
