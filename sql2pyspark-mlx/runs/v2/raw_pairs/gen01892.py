from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@gmail.com"))
    .select("hire_date", "full_name")
    .unionByName(
        df_employees
        .filter(F.col("dept_id") == 40)
        .select("hire_date", "full_name")
    )
    .distinct()
    .orderBy("hire_date")
)
