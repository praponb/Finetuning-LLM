from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser") == "Edge")
    .select("country", "browser", F.initcap(F.trim("browser")).alias("browser_title"))
    .orderBy("browser_title")
)
