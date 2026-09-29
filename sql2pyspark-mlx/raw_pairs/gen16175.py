from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 2506000)
    .groupBy("location")
    .agg(F.count(F.when(F.col("dept_name") == "HR", 1)).alias("hr_count"))
    .filter(F.col("hr_count") < 5)
    .select("location", "hr_count")
)
