from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(F.min("opened_date").alias("first_opened_date"))
    .orderBy(F.col("opened_month").desc())
)
