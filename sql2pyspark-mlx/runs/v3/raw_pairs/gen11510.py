from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "New York")
    .select("id", "dept_name", "location")
    .unionByName(
        df_departments
        .filter(~F.col("location").isin(["Berlin", "Singapore"]))
        .select("id", "dept_name", "location")
    )
)
