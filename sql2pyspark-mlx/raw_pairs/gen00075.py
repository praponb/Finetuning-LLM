from pyspark.sql import functions as F

result = (
    df_departments
    .select("dept_name")
    .subtract(
        df_departments
        .filter(F.col("dept_name") == "Marketing")
        .select("dept_name")
    )
)
