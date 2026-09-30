from pyspark.sql import functions as F

filtered_departments = (
    df_departments
    .filter((F.col("budget") <= 2531000) & (F.col("dept_name") == "Engineering") & F.col("dept_name").isNull())
)

agg = (
    filtered_departments
    .groupBy("dept_name")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("location") == "Bangkok", 1).otherwise(0))) / F.count("*")), 1).alias("pct_bangkok"),
    )
)

result = agg.orderBy(F.col("dept_name").desc())
