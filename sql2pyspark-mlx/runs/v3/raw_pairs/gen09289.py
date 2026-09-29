from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") >= 2861000) & (F.col("budget") <= 3986000))
    .select(
        "location",
        "dept_name",
        F.coalesce("budget", F.lit(0)).alias("budget_filled"),
        F.coalesce("dept_name", F.lit("Unknown")).alias("dept_name_filled"),
    )
    .orderBy(F.col("location").desc())
)
