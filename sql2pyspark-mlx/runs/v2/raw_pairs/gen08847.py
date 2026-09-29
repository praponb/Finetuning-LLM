from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote") & ((F.col("salary") < 89000) | (F.col("salary") >= 110000)))
    .select(
        "id",
        "salary",
        "job_title",
        "level",
        F.substring("email", 1, 3).alias("email_prefix"),
    )
)
