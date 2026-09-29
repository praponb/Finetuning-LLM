from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.month("shipped_date").alias("shipped_month"))
    .agg(F.min("cost").alias("min_cost"))
    .orderBy("shipped_month", "min_cost")
)
