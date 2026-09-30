from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("region") == "Central") & (F.col("sqft") >= 13495))
    .groupBy("city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("region") == "South", 1).otherwise(0))) / F.count("*")), 1).alias("pct_south"),
    )
)
