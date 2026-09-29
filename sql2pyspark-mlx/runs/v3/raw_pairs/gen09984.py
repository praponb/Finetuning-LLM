from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(1372000, 2032000) & ~F.col("location").isin(["Bangkok", "Singapore"]))
    .select(
        "id",
        "dept_name",
        "budget",
        F.substring("dept_name", 1, 2).alias("dept_name_prefix"),
    )
)
