from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Chiang Mai", "Tokyo", "Berlin"]))
    .select("dept_name")
    .unionByName(
        df_departments
        .filter(F.col("location").isNotNull())
        .select("dept_name")
    )
)
