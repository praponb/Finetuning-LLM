from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Singapore", "Tokyo", "Bangkok"]))
    .select("id", "dept_name", "location")
    .unionByName(
        df_departments
        .filter(F.col("budget").between(1326000, 1845000))
        .select("id", "dept_name", "location")
    )
    .orderBy(F.col("dept_name").desc())
)
