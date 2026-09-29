from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 3015000)
    .select("id", "dept_name", "location")
    .unionByName(
        df_departments
        .filter(F.col("location") != "London")
        .select("id", "dept_name", "location")
    )
    .distinct()
)
