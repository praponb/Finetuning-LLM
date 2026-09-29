from pyspark.sql import functions as F

eligible = (
    df_departments
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.manager_id").isNull()), "left_semi")
)

result = (
    eligible
    .select("id", "budget")
    .orderBy(F.col("budget").desc())
)
