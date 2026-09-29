from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("browser") != "Chrome") & F.col("device").isin(["tablet", "mobile"]))
    .orderBy("device")
)
