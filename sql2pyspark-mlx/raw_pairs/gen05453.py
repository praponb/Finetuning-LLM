from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date") >= "2023-09-01")
    .select("email", "manager_id")
    .unionByName(
        df_employees
        .filter(F.col("dept_id").isin([30, 36]))
        .select("email", "manager_id")
    )
    .distinct()
    .orderBy("email")
)
