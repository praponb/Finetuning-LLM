from pyspark.sql import functions as F

result = (
    df_departments
    .select("location", "dept_name")
    .subtract(
        df_departments
        .filter(F.col("dept_name").isin(["Sales", "Legal"]))
        .select("location", "dept_name")
    )
)
