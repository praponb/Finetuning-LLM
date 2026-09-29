from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "New York")
    .select("id", "location")
    .unionByName(
        df_departments
        .filter(F.col("budget") <= 1508000)
        .select("id", "location")
    )
    .distinct()
    .orderBy("id")
)
