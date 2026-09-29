from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") == "Finance")
    .select("id", "location", "dept_name")
    .intersect(
        df_departments
        .filter(F.col("budget") >= 2400000)
        .select("id", "location", "dept_name")
    )
)
