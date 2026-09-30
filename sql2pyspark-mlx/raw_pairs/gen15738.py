from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isNotNull() & (~F.col("email").like("%@mail.co.th") | (F.col("hire_date") > "2023-05-17")))
    .select("dept_id", "email", "is_remote")
    .orderBy("email", F.col("dept_id").desc())
)
