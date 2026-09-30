from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id") == 15)
    .select(
        "id",
        "hire_date",
        F.upper(F.trim("email")).alias("email_normalized"),
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
)
