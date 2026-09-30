from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(~F.col("is_remote"))
    .groupBy("dept_id")
    .agg(
        F.countDistinct("salary").alias("unique_levels"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "left")
    .filter(F.col("dt.unique_levels") >= 7)
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("dt.unique_levels"),
        F.col("dt.avg_salary"),
    )
)
