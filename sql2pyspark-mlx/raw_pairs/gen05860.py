from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "budget",
        F.coalesce("budget", F.lit(0)).alias("budget_filled"),
        F.concat("location", F.lit(", "), "dept_name").alias("location_dept_name"),
    )
    .orderBy(F.col("location_dept_name").desc())
)
