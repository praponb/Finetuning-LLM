from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") == "Finance")
    .select("id", "location", "dept_name")
    .unionByName(
        df_departments
        .filter(F.col("budget").between(1300000, 2546000))
        .select("id", "location", "dept_name")
    )
)
