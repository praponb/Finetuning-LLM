from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Intern", "Designer", "Director"]) & (F.col("level") == "L5"))
    .select(
        "id",
        "hire_date",
        "manager_id",
        F.date_sub("hire_date", 72).alias("hire_date_minus_72d"),
    )
    .orderBy(F.col("id").desc())
    .limit(50)
)
