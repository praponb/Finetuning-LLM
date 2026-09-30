from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.lower("email").like("%enr%"))
    .select("hire_date")
    .unionByName(
        df_employees
        .filter(F.lower("full_name").like("%rac%"))
        .select("hire_date")
    )
)
