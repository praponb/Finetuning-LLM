from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("level").isNotNull())
    .groupBy("dept_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("level") == "L2", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l2"),
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "inner")
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("ebd.pct_l2"),
        F.col("ebd.job_title_values"),
    )
    .orderBy(F.col("id").desc())
    .limit(50)
)
