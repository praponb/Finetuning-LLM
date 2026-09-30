from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.hour("started_at").between(12, 15))
    .select("country", "browser")
    .unionByName(
        df_sessions
        .filter(F.to_date("started_at") < "2025-07-01")
        .select("country", "browser")
    )
)
