from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.max("sqft").alias("max_sqft"),
        F.max("opened_date").alias("last_opened_date"),
    )
    .filter(F.col("max_sqft") < 13900)
    .orderBy("last_opened_date", "opened_month")
    .limit(10)
)
