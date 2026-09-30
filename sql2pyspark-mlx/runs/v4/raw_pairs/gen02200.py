from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date") <= "2025-09-01")
    .select("id", "manager_id", "email")
    .unionByName(
        df_employees
        .filter(F.col("salary").between(118000, 127000))
        .select("id", "manager_id", "email")
    )
    .distinct()
    .orderBy("manager_id")
)
