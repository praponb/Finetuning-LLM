from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") == 7856)
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.sum(F.when(F.col("city") == "Chiang Mai", 1).otherwise(0)).alias("chiang_mai_count"),
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
    )
    .filter(F.col("sqft_range") > 67)
    .orderBy("sqft_range", "opened_month")
    .limit(50)
)
