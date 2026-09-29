from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Bangkok", "Berlin", "Chiang Mai"]))
    .select(
        "id",
        "budget",
        "location",
        "dept_name",
        F.coalesce("budget", F.lit(0)).alias("budget_filled"),
    )
    .orderBy("dept_name")
)
