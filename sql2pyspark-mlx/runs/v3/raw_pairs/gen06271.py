from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Intern", "Manager"]))
    .select("id", "level", "dept_id")
    .unionByName(
        df_employees
        .filter(F.upper(F.trim("full_name")) == "BOB SMITH")
        .select("id", "level", "dept_id")
    )
    .distinct()
    .orderBy("level")
)
