from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("job_title").isNull())
    .groupBy("dept_id")
    .agg(
        F.countDistinct("salary").alias("unique_managers"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "left")
    .select(
        F.col("d.dept_name"),
        F.col("ds.unique_managers"),
        F.col("ds.stddev_salary"),
    )
)
