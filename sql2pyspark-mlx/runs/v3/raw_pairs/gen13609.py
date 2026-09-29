from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") == "Engineering")
    .select("dept_name", "location")
    .unionByName(
        df_departments
        .filter(F.col("location") == "Singapore")
        .select("dept_name", "location")
    )
    .distinct()
)
