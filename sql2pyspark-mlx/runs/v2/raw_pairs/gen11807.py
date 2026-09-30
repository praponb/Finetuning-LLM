from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") > 3247000)
    .select("location", "dept_name")
    .intersect(
        df_departments
        .filter(~F.col("location").isin(["Bangkok", "Singapore", "London"]))
        .select("location", "dept_name")
    )
)
