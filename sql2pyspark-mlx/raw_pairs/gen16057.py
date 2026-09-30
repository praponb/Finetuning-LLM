from pyspark.sql import functions as F

result = (
    df_payments
    .select(F.lit("payment").alias("source"), "method")
    .union(df_departments.select(F.lit("department").alias("source"), "location"))
    .distinct()
    .orderBy(F.col("source").desc())
)
