from pyspark.sql import functions as F

result = (
    df_departments
    .select("id", "location")
    .subtract(
        df_departments
        .filter(F.col("dept_name").isin(["Engineering", "Finance"]))
        .select("id", "location")
    )
)
