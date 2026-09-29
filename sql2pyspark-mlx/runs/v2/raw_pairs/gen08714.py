from pyspark.sql import functions as F

result = (
    df_departments
    .cube("location", "dept_name")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("location") == "Chiang Mai", 1).otherwise(0))) / F.count("*")), 1).alias("pct_chiang_mai"),
        F.count("budget").alias("budget_count"),
    )
    .orderBy("dept_name")
)
