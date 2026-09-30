from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.upper("email").like("%ANY%"))
    .select(
        "id",
        "level",
        "salary",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.substring("email", 1, 3).alias("email_prefix"),
    )
)
