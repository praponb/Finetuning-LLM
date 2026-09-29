from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") > 2453000)
    .select("location")
    .unionByName(
        df_departments
        .filter(F.col("location") == "Chiang Mai")
        .select("location")
    )
    .distinct()
)
