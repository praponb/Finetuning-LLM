from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("cost") <= 83) & (F.col("shipped_date") > "2023-09-01"))
    .agg(F.sum("cost").alias("total_cost"))
    .orderBy(F.col("total_cost").desc())
)
