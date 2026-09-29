from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("dept_id").isin([10, 14]))
    .groupBy("dept_id")
    .agg(
        F.count(F.when(F.col("job_title") == "Engineer", 1)).alias("engineer_count"),
        F.count(F.when(F.col("level") == "L1", 1)).alias("l1_count"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("d.location"),
        F.col("t.engineer_count"),
        F.col("t.l1_count"),
    )
    .orderBy(F.col("l1_count").desc())
)
