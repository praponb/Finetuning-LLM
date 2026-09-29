from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("location").isin(["Berlin", "Bangkok", "London"]))
    .select("dept_name", "location")
    .unionByName(
        df_departments
        .filter(F.col("budget") < 1659000)
        .select("dept_name", "location")
    )
    .distinct()
    .orderBy("location")
)
