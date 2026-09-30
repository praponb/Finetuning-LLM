from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") == "Marketing")
    .select("location", "dept_name")
    .subtract(
        df_departments
        .filter(F.col("dept_name") == "Engineering")
        .select("location", "dept_name")
    )
)
