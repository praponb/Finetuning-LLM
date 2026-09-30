from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date").between("2023-04-01", "2025-02-01"))
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(F.min("sqft").alias("min_sqft"))
    .filter(F.col("min_sqft") >= 15400)
    .orderBy(F.col("min_sqft").desc(), F.col("opened_month").desc())
)
