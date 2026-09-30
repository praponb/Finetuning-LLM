from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") == "Sales")
    .select("location", "dept_name")
    .unionByName(
        df_departments
        .filter(F.col("dept_name").isNotNull())
        .select("location", "dept_name")
    )
    .orderBy("location")
)
