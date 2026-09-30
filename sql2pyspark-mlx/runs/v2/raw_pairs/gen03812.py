from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") >= 2851000) & (F.col("location") == "Berlin") & F.col("location").isNotNull())
    .select("budget", "location")
)
