from pyspark.sql import functions as F

valid = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("budget_whole", F.ceil("budget"))
)

result = (
    valid
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.job_title").isin(["Engineer", "Analyst"])), "left_anti")
)
