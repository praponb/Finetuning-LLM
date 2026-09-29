from pyspark.sql import functions as F

filtered_stores = df_stores.filter(F.year("opened_date") == 2025)

agg = (
    filtered_stores
    .groupBy(F.quarter("opened_date").alias("opened_quarter"), "city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Berlin", 1).otherwise(0))) / F.count("*")), 1).alias("pct_berlin"),
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_count"),
    )
)

result = (
    agg
    .orderBy(F.col("bangkok_count").desc(), "opened_quarter", "city")
    .limit(20)
)
