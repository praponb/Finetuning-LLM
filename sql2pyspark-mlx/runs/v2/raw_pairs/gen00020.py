from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "London", 1).otherwise(0))) / F.count("*")), 1).alias("pct_london"),
    )
    .filter(F.col("pct_london") < 47)
    .select("opened_month", "pct_london")
    .orderBy(F.col("pct_london").desc(), "opened_month")
    .limit(1)
)
