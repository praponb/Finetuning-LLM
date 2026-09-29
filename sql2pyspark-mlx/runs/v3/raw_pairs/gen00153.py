from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Marketing", "Engineering"]))
    .select("id", "location", "dept_name")
    .unionByName(
        df_departments
        .filter(F.col("location").isNotNull())
        .select("id", "location", "dept_name")
    )
    .distinct()
)
