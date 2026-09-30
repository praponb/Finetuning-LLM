from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") <= "2023-03-01")
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Berlin", 1).otherwise(0))) / F.count("*")), 1).alias("pct_berlin"),
        F.min("sqft").alias("min_sqft"),
    )
    .filter(F.col("min_sqft") < 6300)
    .select("opened_month", "pct_berlin", "min_sqft")
    .orderBy(F.col("opened_month").desc(), F.col("min_sqft").desc())
)
