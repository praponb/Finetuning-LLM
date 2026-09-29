from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Bangkok")
    .select("id", "location")
    .unionByName(
        df_departments
        .filter(F.col("location").isin(["Bangkok", "Singapore", "Tokyo"]))
        .select("id", "location")
    )
)
