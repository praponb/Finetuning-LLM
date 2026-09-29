from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "New York")
    .select("dept_name")
    .unionByName(
        df_departments
        .filter(F.col("dept_name") == "Engineering")
        .select("dept_name")
    )
    .distinct()
)
