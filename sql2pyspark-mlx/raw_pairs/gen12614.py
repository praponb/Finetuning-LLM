from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Singapore")
    .select("id", "location")
    .unionByName(
        df_departments
        .filter(F.col("budget") <= 1876000)
        .select("id", "location")
    )
    .distinct()
)
