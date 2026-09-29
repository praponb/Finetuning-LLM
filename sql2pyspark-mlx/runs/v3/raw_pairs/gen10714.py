from pyspark.sql import functions as F

src = (
    df_stores
    .filter((F.col("city") == "Chiang Mai") & (F.col("city") == "Bangkok") & F.col("store_name").isNotNull())
)

summary = (
    src
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(F.min("sqft").alias("min_sqft"))
)

result = summary.select("opened_month", "min_sqft")
