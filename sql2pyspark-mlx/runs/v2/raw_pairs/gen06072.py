from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Engineering", "Marketing", "Legal"]))
    .select("location")
    .unionByName(
        df_departments
        .filter(F.col("dept_name").isin(["HR", "Finance"]))
        .select("location")
    )
)
