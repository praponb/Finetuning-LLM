from pyspark.sql import functions as F

recent_departments = (
    df_departments
    .filter(F.col("budget").between(2167000, 3617000) & ~F.col("location").isin(["Bangkok", "Berlin", "New York"]) & (F.col("budget") < 4011000))
)

departments_stats = (
    recent_departments
    .groupBy("dept_name")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("location") == "New York", 1).otherwise(0))) / F.count("*")), 1).alias("pct_new_york"),
        F.countDistinct("budget").alias("unique_dept_names"),
    )
)

result = (
    departments_stats
    .select("dept_name", "pct_new_york", "unique_dept_names")
    .orderBy("dept_name")
)
