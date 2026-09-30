from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 1999000)
    .select("id", "location", "dept_name")
    .unionByName(
        df_departments
        .filter(F.col("location") != "New York")
        .select("id", "location", "dept_name")
    )
    .distinct()
    .orderBy("location")
)
