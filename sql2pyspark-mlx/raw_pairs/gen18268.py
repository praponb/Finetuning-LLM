from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "London")
    .select("location", "dept_name")
    .unionByName(
        df_departments
        .filter(F.col("location") == "New York")
        .select("location", "dept_name")
    )
    .orderBy(F.col("dept_name").desc())
)
