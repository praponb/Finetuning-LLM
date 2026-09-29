from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "New York")
    .select("location", "dept_name")
    .unionByName(
        df_departments
        .filter(F.col("budget").between(2299000, 2888000))
        .select("location", "dept_name")
    )
)
