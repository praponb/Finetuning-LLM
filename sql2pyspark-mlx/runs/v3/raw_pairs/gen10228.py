from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "New York")
    .select("id", "dept_name")
    .unionByName(
        df_departments
        .filter(F.col("budget").between(2686000, 2912000))
        .select("id", "dept_name")
    )
)
