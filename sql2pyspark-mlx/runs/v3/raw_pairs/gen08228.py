from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Chiang Mai")
    .select("id", "dept_name", "location")
    .unionByName(
        df_departments
        .filter(F.col("dept_name") == "HR")
        .select("id", "dept_name", "location")
    )
    .distinct()
)
