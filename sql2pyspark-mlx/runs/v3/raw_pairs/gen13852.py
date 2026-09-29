from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.count("*").alias("total_count"),
        F.min("hire_date").alias("first_hire_date"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "inner")
    .filter(F.col("dt.total_count") >= 7)
    .select(F.col("d.budget"), F.col("dt.total_count"), F.col("dt.first_hire_date"))
    .orderBy(F.col("total_count").desc(), "first_hire_date")
)
