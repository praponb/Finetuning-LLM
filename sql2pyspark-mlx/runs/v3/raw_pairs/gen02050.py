from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("browser") == "Chrome") & F.col("device").isin(["desktop", "mobile"]))
    .orderBy("browser", "id")
    .limit(10)
)
