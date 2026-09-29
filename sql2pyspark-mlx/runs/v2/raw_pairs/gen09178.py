from pyspark.sql import functions as F

department_stats = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.countDistinct("salary").alias("unique_depts"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .filter(F.col("ds.unique_depts") >= 6)
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("d.budget"),
        F.col("ds.unique_depts"),
        F.col("ds.stddev_salary"),
    )
    .orderBy(F.col("stddev_salary").desc(), "id")
    .limit(10)
)
