from pyspark.sql import functions as F

recent_stores = df_stores.filter(~F.col("city").isin(["Berlin", "Chiang Mai"]))

agg = (
    recent_stores
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0))) / F.count("*")), 1).alias("pct_tokyo"),
        F.max("opened_date").alias("last_opened_date"),
    )
)

result = (
    agg
    .orderBy(F.col("last_opened_date").desc(), F.col("opened_month").desc())
    .limit(20)
)
