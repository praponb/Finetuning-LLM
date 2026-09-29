from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") == "Sales")
    .select("id", "dept_name", "location")
    .subtract(
        df_departments
        .filter(F.col("dept_name") != "Marketing")
        .select("id", "dept_name", "location")
    )
)
