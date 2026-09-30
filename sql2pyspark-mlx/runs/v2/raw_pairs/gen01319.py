from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 2990000)
    .select("location", "dept_name")
    .intersect(
        df_departments
        .filter(~F.col("dept_name").isin(["Marketing", "Sales"]))
        .select("location", "dept_name")
    )
)
