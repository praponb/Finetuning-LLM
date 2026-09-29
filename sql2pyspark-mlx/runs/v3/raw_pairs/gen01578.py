from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title") == "Manager")
    .select("manager_id", "email")
    .unionByName(
        df_employees
        .filter(F.col("salary").between(70000, 100000))
        .select("manager_id", "email")
    )
    .distinct()
    .orderBy(F.col("email").desc())
)
