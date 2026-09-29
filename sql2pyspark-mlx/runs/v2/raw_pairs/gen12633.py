from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") > 1867000)
    .select("location", "dept_name")
    .intersect(
        df_departments
        .filter(F.col("dept_name") == "Engineering")
        .select("location", "dept_name")
    )
)
