from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_count"),
        F.min("sqft").alias("min_sqft"),
    )
    .orderBy(F.col("opened_month").desc())
)
