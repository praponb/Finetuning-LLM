from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("started_at") < "2023-12-01")
    .select("id", "country")
    .unionByName(
        df_sessions
        .filter(F.col("started_at") < "2024-02-23")
        .select("id", "country")
    )
    .distinct()
)
