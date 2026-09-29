from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("email").like("%@mail.co.th"))
    .select("dept_id", "email", "manager_id")
)
