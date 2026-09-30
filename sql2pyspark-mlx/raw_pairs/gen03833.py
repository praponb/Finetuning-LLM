from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Edge", "Safari"]) | (F.col("device") == "mobile"))
    .orderBy(F.col("id").desc())
)
