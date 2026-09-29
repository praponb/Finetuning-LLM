from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") >= 4879)
    .groupBy(
        F.year("opened_date").alias("opened_year"),
        F.date_format("opened_date", "yyyy-MM").alias("opened_month"),
    )
    .agg(
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
    )
    .filter(F.col("sqft_range") >= 33)
    .orderBy("sqft_range", "opened_year", "opened_month")
    .limit(10)
)
