from pyspark.sql import functions as F

recent_employees = (
    df_employees
    .filter(F.col("full_name").like("%Sri%") & F.col("salary").between(84000, 116000))
)

hire_month_summary = (
    recent_employees
    .groupBy(F.month("hire_date").alias("hire_month"))
    .agg(
        F.sum(F.when(F.col("level") == "L1", F.col("salary")).otherwise(0)).alias("l1_salary"),
    )
)

result = (
    hire_month_summary
    .filter(F.col("l1_salary") >= 55)
    .select("hire_month", "l1_salary")
    .orderBy(F.col("l1_salary").desc())
)
