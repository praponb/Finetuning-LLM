from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isin([24, 28]))
    .select("id", "level", "hire_date")
    .unionByName(
        df_employees
        .filter(F.col("job_title").isin(["Intern", "Engineer", "Manager"]))
        .select("id", "level", "hire_date")
    )
    .distinct()
    .orderBy("level")
)
