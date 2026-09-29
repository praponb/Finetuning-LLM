from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("level") == "L2")
    .groupBy("dept_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
        F.max("salary").alias("max_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "inner")
    .filter(F.col("dt.max_salary") > 116000)
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("dt.level_values"),
        F.col("dt.max_salary"),
    )
)
