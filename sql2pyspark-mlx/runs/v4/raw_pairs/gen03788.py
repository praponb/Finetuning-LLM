from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Safari", "Edge"]))
    .select("browser", "country")
    .unionByName(
        df_sessions
        .filter(F.to_date("started_at") < "2025-02-01")
        .select("browser", "country")
    )
)
