from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country") == "US")
    .select("country")
    .subtract(df_sessions.filter(F.col("browser") == "Chrome").select("country"))
)
