from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 3894000)
    .select("dept_name", "location")
    .subtract(
        df_departments
        .filter(F.col("budget") > 2693000)
        .select("dept_name", "location")
    )
    .orderBy(F.col("location").desc())
)
