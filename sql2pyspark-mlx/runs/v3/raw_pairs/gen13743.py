from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "email",
        "level",
        "salary",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
    .orderBy(F.col("id").desc())
)
