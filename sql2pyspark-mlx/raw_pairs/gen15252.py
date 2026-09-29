from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").isNotNull())
    .select("location")
    .unionByName(
        df_departments
        .filter(F.col("location") == "Singapore")
        .select("location")
    )
)
