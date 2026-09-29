from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Berlin")
    .select("id", "location", "dept_name")
    .unionByName(
        df_departments
        .filter(F.col("budget") > 3932000)
        .select("id", "location", "dept_name")
    )
)
