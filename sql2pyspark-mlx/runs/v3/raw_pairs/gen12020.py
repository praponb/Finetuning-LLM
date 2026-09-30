from pyspark.sql import functions as F

valid_departments = df_departments.filter(F.col("budget").isNull())

agg = (
    valid_departments
    .groupBy("location")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "Engineering", 1).otherwise(0))) / F.count("*")), 1).alias("pct_engineering"),
    )
)

result = (
    agg
    .filter(F.col("pct_engineering") >= 49)
    .orderBy(F.col("pct_engineering").desc(), "location")
    .limit(10)
)
