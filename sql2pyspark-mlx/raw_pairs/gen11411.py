from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
        F.countDistinct("salary").alias("unique_managers"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("D"), F.col("d.id") == F.col("D.dept_id"), "inner")
    .filter(F.col("D.unique_managers") >= 6)
    .select(
        F.col("d.dept_name"),
        F.col("D.job_title_values"),
        F.col("D.unique_managers"),
    )
)
