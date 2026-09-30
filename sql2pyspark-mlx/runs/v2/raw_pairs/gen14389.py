from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") == "Marketing")
    .select("dept_name")
    .unionByName(
        df_departments
        .filter(F.col("location").isin(["Berlin", "Tokyo", "London"]))
        .select("dept_name")
    )
)
