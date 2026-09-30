from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([2, 21, 30]))
    .select("hire_date", "dept_id")
    .unionByName(
        df_employees
        .filter(F.col("dept_id").isin([8, 12, 29, 38]))
        .select("hire_date", "dept_id")
    )
    .distinct()
)
