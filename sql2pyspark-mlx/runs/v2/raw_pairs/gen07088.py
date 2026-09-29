from pyspark.sql import functions as F

src = (
    df_departments
    .filter(~F.col("location").isin(["Singapore", "Berlin"]) & (F.col("budget") > 2918000))
)

summary = (
    src
    .groupBy("dept_name")
    .agg(
        F.count(F.when(F.col("location") == "Berlin", 1)).alias("berlin_count"),
        F.round(((100 * F.sum(F.when(F.col("location") == "New York", 1).otherwise(0))) / F.count("*")), 1).alias("pct_new_york"),
    )
)

result = summary
