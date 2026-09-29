from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@outlook.com"))
    .select("signup_date")
    .unionByName(
        df_employees
        .filter(F.col("dept_id").isin([10, 30]))
        .select(F.col("hire_date").alias("signup_date"))
    )
    .distinct()
)
