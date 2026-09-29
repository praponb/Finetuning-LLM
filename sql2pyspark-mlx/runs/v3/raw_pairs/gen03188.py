from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Manager", 1).otherwise(0))) / F.count("*")), 1).alias("pct_manager"),
        F.countDistinct("salary").alias("unique_managers"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "left")
    .filter(F.col("dt.pct_manager") > 55)
    .select(
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.col("dt.pct_manager"),
        F.col("dt.unique_managers"),
    )
    .orderBy(F.col("pct_manager").desc(), F.col("unique_managers").desc())
)
