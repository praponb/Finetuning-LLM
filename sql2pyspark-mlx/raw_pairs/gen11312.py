from pyspark.sql import functions as F

eligible = (
    df_departments
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (~F.col("ch.job_title").isin(["Designer", "Intern"])), "left_semi")
)

result = (
    eligible
    .select("id", "dept_name", "location")
    .orderBy(F.col("dept_name").desc_nulls_first())
)
