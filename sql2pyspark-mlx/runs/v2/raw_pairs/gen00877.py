from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(
        F.min("opened_date").alias("first_opened_date"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.count("sqft").alias("sqft_count"),
    )
    .orderBy(F.col("opened_month").desc())
)
