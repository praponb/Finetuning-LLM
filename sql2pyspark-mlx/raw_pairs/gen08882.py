from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(2052000, 2287000))
    .select("location")
    .unionByName(
        df_departments
        .filter(F.col("location").isin(["Singapore", "Tokyo", "New York"]))
        .select("location")
    )
    .distinct()
    .orderBy("location")
)
