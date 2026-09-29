from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("region") == "North") | (F.col("opened_date") >= "2023-10-01"))
    .groupBy("city", "region")
    .agg(
        F.sum("sqft").alias("total_sqft"),
        F.round(((100 * F.sum(F.when(F.col("region") == "Central", 1).otherwise(0))) / F.count("*")), 1).alias("pct_central"),
    )
    .orderBy("city", "pct_central")
)
