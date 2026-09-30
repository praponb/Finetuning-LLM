from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").between("2023-05-29", "2025-06-15"))
    .select("id", "level")
    .unionByName(
        df_employees
        .filter(F.col("hire_date") >= "2023-12-01")
        .select("id", "level")
    )
    .orderBy("id")
)
