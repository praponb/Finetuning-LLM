from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Edge", "Chrome", "Safari"]))
    .select("country")
    .unionByName(df_sessions.filter(F.col("country").isNotNull()).select("country"))
)
