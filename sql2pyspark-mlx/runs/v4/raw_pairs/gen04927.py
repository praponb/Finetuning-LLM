from pyspark.sql import functions as F

result = (
    df_sessions
    .select("browser", "country")
    .exceptAll(
        df_sessions
        .filter(F.col("country").isin(["DE", "US", "TH"]))
        .select("browser", "country")
    )
)
