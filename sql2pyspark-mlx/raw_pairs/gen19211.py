from pyspark.sql import functions as F

eligible = (
    df_departments
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.manager_id") == 36), "left_semi")
)

result = (
    eligible
    .select("id", "dept_name", "location", "budget")
    .orderBy(F.col("dept_name").desc())
)
