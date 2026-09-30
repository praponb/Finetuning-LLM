from pyspark.sql import functions as F

result = (
    df_sessions
    .select("id")
    .subtract(
        df_sessions
        .filter(F.col("device") != "mobile")
        .select("id")
    )
)
