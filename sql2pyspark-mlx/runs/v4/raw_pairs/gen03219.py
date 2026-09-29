from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("is_remote"))
    .select("email", "job_title")
    .unionByName(
        df_employees
        .filter(F.col("dept_id") == 3)
        .select("email", "job_title")
    )
    .distinct()
    .orderBy("job_title")
)
