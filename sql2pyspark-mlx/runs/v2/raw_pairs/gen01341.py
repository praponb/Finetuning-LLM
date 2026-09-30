from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.upper("full_name").like("%MMA%"))
    .groupBy("dept_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("level") == "L1", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l1"),
        F.countDistinct("salary").alias("unique_depts"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "left")
    .filter(F.col("ebd.pct_l1") >= 45)
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("ebd.pct_l1"),
        F.col("ebd.unique_depts"),
    )
    .orderBy(F.col("unique_depts").desc())
)
