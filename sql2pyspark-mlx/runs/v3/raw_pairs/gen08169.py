from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.date_format("shipped_date", "yyyy-MM").alias("shipped_month"))
    .agg(F.sum("cost").alias("total_cost"))
    .filter(F.col("total_cost") > 460)
    .orderBy(F.col("total_cost").desc())
)
