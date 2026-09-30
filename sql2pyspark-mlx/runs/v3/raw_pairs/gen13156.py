from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") != "Chiang Mai") & (F.col("location") != "London") & F.col("location").isin(["Bangkok", "Tokyo"]))
    .orderBy("location")
)
