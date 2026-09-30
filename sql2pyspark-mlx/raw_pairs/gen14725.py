from pyspark.sql import functions as F

cleaned = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .select("*", F.length("dept_name").alias("dept_name_length"))
)

result = (
    cleaned
    .select("id", "location")
    .intersect(
        df_departments
        .filter(F.col("budget") <= 2976000)
        .select("id", "location")
    )
)
