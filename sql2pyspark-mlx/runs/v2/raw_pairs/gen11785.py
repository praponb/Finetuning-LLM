from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") <= 11230) & F.col("opened_date").between("2023-03-03", "2024-02-01"))
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.count("sqft").alias("sqft_count"),
        F.min("sqft").alias("min_sqft"),
    )
    .filter(F.col("min_sqft") < 10000)
    .drop("min_sqft")
    .orderBy("opened_month", "sqft_count")
)
