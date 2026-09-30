from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "email",
        "manager_id",
        "is_remote",
        F.initcap(F.trim("email")).alias("email_title"),
    )
    .orderBy(F.col("email").desc())
)
