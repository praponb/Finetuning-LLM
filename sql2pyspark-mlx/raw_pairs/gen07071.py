from pyspark.sql import functions as F

result = (
    df_departments
    .select("dept_name", "id")
    .exceptAll(
        df_departments
        .filter(~F.col("dept_name").isin(["Marketing", "Engineering"]))
        .select("dept_name", "id")
    )
)
