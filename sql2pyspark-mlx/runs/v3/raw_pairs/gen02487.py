from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("email").like("%@mail.co.th") & F.col("salary").between(73000, 109000) & (F.col("hire_date") < "2025-07-01"))
    .select(
        "id",
        "hire_date",
        "full_name",
        "job_title",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.lower("level").alias("level_lower"),
    )
)
