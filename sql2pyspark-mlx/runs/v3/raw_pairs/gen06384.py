from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary").between(94000, 134000))
    .select("id", "email", "full_name")
    .unionByName(
        df_employees
        .filter(F.year("hire_date") >= 2024)
        .select("id", "email", "full_name")
    )
    .orderBy(F.col("email").desc())
)
