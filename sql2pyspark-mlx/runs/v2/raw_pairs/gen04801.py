from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 3278000)
    .select("dept_name", "location")
    .unionByName(
        df_departments
        .filter(F.col("budget") < 1838000)
        .select("dept_name", "location")
    )
    .distinct()
)
