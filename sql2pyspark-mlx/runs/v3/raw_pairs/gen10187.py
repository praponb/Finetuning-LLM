from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("is_remote"))
    .select("id", "job_title", "level")
    .unionByName(
        df_employees
        .filter(F.col("level") == "L2")
        .select("id", "job_title", "level")
    )
)
