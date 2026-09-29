from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote"))
    .select(
        "id",
        "email",
        F.datediff(F.current_date(), "hire_date").alias("days_since_hire_date"),
        F.initcap(F.trim("level")).alias("level_title"),
    )
)
