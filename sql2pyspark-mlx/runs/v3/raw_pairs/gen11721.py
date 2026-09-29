from pyspark.sql import functions as F

filtered_stores = (
    df_stores
    .filter(~((F.col("opened_date") > "2023-11-01") | (F.year("opened_date") == 2025)))
)

agg = (
    filtered_stores
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"), "region")
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.min("opened_date").alias("first_opened_date"),
    )
)

result = (
    agg
    .filter(F.col("avg_sqft") < 66)
    .select("opened_month", "region", "avg_sqft", "first_opened_date")
)
