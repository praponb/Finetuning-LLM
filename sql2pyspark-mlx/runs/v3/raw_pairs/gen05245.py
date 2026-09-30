from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") == "Finance")
    .select(
        "id",
        "location",
        F.lower("location").alias("location_lower"),
        F.round("budget", 1).alias("budget_rounded"),
    )
    .orderBy(F.col("id").desc())
)
