from pyspark.sql import functions as F

valid_stores = df_stores.filter(F.col("region").isin(["South", "North"]))

summary = (
    valid_stores
    .groupBy("city", F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
    )
)

result = (
    summary
    .select("city", "opened_month", "avg_sqft", "sqft_range")
    .orderBy(F.col("avg_sqft").desc())
)
