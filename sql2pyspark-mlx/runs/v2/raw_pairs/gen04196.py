from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date") < "2023-03-01")
    .select("id", "full_name")
    .unionByName(
        df_employees
        .filter(F.col("hire_date") >= "2024-08-26")
        .select("id", "full_name")
    )
)
