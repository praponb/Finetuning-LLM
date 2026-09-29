from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(
        F.sum("sqft").alias("total_sqft"),
        F.round(((100 * F.sum(F.when(F.col("region") == "Central", 1).otherwise(0))) / F.count("*")), 1).alias("pct_central"),
    )
    .filter(F.col("pct_central") >= 47)
    .orderBy(F.col("total_sqft").desc())
)
