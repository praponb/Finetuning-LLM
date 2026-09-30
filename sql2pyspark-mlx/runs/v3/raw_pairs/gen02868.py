from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("store_name").like("%Express%"))
    .select(
        "id",
        "city",
        F.date_sub("opened_date", 41).alias("opened_date_minus_41d"),
        F.when(F.col("sqft") >= 14983, "High").when(F.col("sqft") >= 6101, "Medium").otherwise("Low").alias("sqft_level"),
    )
    .orderBy(F.col("id").desc())
    .limit(5)
)
