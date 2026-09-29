from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "HR") & (F.col("location").isin(["Bangkok", "Berlin"]) | (F.col("budget") <= 3764000)))
    .select(
        "budget",
        F.round("budget", 0).alias("budget_rounded"),
        F.trim("location").alias("clean_location"),
    )
    .orderBy(F.col("budget").desc())
)
