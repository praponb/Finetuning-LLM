from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.count("sqft").alias("sqft_count"),
        F.count("*").alias("row_count"),
    )
    .orderBy("row_count", "opened_month")
    .limit(10)
)
