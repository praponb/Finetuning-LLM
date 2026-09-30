from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 2723000)
    .select("dept_name", "location")
    .subtract(
        df_departments
        .filter(F.col("location").isin(["Chiang Mai", "Bangkok", "Berlin"]))
        .select("dept_name", "location")
    )
)
