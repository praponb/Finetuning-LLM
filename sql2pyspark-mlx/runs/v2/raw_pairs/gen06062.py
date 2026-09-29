from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("location") == "New York", 1).otherwise(0))) / F.count("*")), 1).alias("pct_new_york"),
    )
    .filter(F.col("pct_new_york") >= 31)
    .select("location", "pct_new_york")
    .orderBy("location")
    .limit(1)
)
