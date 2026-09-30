from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") != "London")
    .select("id", "dept_name")
    .unionByName(
        df_departments
        .filter(F.col("location").isin(["New York", "Bangkok"]))
        .select("id", "dept_name")
    )
    .orderBy(F.col("dept_name").desc())
)
