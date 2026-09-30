from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title") != "Intern")
    .select("hire_date", "manager_id")
    .unionByName(
        df_employees
        .filter(F.col("level").isin(["L4", "L5", "L1"]))
        .select("hire_date", "manager_id")
    )
    .distinct()
    .orderBy("manager_id")
)
