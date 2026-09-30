from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select("id", "location", "dept_name")
    .unionByName(
        df_departments
        .filter(F.col("location") == "Tokyo")
        .select("id", "location", "dept_name")
    )
    .orderBy("dept_name")
)
