from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("t"), F.col("d.id") == F.col("t.dept_id"), "left")
    .filter(F.col("t.avg_salary") > 33)
    .select(
        F.col("d.location"),
        F.col("d.budget"),
        F.col("t.avg_salary"),
        F.col("t.level_list"),
    )
    .orderBy(F.col("location").desc())
)
