from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") > 2045000)
    .select("dept_name", "location")
    .unionByName(
        df_departments
        .filter(F.col("dept_name").isin(["HR", "Marketing", "Engineering"]))
        .select("dept_name", "location")
    )
    .distinct()
)
