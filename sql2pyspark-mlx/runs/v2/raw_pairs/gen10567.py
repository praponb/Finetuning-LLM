from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title") == "Manager")
    .select("manager_id")
    .intersect(
        df_employees
        .filter(F.col("job_title") != "Designer")
        .select("manager_id")
    )
    .orderBy(F.col("manager_id").desc())
)
