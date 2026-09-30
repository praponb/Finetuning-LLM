from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date") < "2025-07-01")
    .select("id", "hire_date")
    .unionByName(
        df_employees
        .filter(F.col("manager_id").isNull())
        .select("id", "hire_date")
    )
    .orderBy("hire_date")
)
