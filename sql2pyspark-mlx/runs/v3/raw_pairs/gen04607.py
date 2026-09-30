from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@startup.io"))
    .select(
        "level",
        "is_remote",
        "email",
        F.upper("full_name").alias("full_name_upper"),
    )
    .orderBy(F.col("level").desc())
)
