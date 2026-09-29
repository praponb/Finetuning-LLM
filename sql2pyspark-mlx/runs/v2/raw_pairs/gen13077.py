from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").isNotNull())
    .groupBy("city")
    .agg(
        F.min("opened_date").alias("first_opened_date"),
        F.round(((100 * F.sum(F.when(F.col("region") == "South", 1).otherwise(0))) / F.count("*")), 1).alias("pct_south"),
    )
    .orderBy("first_opened_date", "city")
    .limit(3)
)
