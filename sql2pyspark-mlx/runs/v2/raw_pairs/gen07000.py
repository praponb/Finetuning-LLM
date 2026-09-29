from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Engineering", "Marketing"]))
    .select("dept_name", "location")
    .subtract(
        df_departments
        .filter(F.col("budget") < 3801000)
        .select("dept_name", "location")
    )
    .orderBy("dept_name")
)
