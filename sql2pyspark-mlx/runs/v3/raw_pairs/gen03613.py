from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "left")
    .groupBy(
        F.col("d.dept_name"),
        F.when(F.col("d.dept_name").isin(["Finance", "Marketing"]), "core").otherwise("other").alias("dept_name_group"),
    )
    .agg(
        F.count("*").alias("employee_count"),
        F.max(F.col("e.salary")).alias("max_salary"),
    )
    .filter(F.col("max_salary") >= 107000)
    .orderBy(
        "max_salary",
        F.col("employee_count").desc(),
        "dept_name",
        "dept_name_group",
    )
    .limit(50)
)
