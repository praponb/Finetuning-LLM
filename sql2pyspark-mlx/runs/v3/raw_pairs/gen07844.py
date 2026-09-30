from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@outlook.com"))
    .select("id", "full_name")
    .intersect(
        df_employees
        .filter(F.col("hire_date") > "2023-06-26")
        .select("id", "full_name")
    )
)
