from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("manager_id").isin([16, 24]))
    .groupBy("dept_id")
    .agg(
        F.countDistinct("salary").alias("unique_levels"),
        F.min("hire_date").alias("first_hire_date"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("t"), F.col("d.id") == F.col("t.dept_id"), "left")
    .filter(F.col("t.unique_levels") >= 4)
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("d.location"),
        F.col("t.unique_levels"),
        F.col("t.first_hire_date"),
    )
    .orderBy(F.col("unique_levels").desc())
)
