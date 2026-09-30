from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") < 3261000)
    .select("id", "location", "dept_name")
    .unionByName(
        df_departments
        .filter(F.col("budget") < 2043000)
        .select("id", "location", "dept_name")
    )
    .distinct()
)
