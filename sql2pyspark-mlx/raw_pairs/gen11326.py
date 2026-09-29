from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("dept_id").isin([2, 10, 13, 16]))
    .groupBy("dept_id")
    .agg(
        F.max("hire_date").alias("last_hire_date"),
        F.countDistinct("salary").alias("unique_levels"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "inner")
    .filter(F.col("dt.unique_levels") > 4)
    .select(F.col("d.budget"), F.col("dt.last_hire_date"), F.col("dt.unique_levels"))
    .orderBy("unique_levels")
)
