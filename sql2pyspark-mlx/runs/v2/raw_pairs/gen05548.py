from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.year("opened_date") >= 2023)
    .groupBy(F.month("opened_date").alias("opened_month"), "city")
    .agg(
        F.sum(F.when(F.col("region") == "Central", 1).otherwise(0)).alias("central_count"),
    )
    .filter(F.col("central_count") < 23)
    .orderBy(F.col("opened_month").desc())
)
