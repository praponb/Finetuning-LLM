from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("level") == "L3", 1).otherwise(0)).alias("l3_count"),
        F.sum(F.when(F.col("job_title") == "Engineer", F.col("salary")).otherwise(0)).alias("engineer_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "inner")
    .select(F.col("d.dept_name"), F.col("dt.l3_count"), F.col("dt.engineer_salary"))
)
