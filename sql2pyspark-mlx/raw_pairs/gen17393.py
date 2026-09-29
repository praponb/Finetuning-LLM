from pyspark.sql import functions as F

result = (
    df_sessions
    .select("id", "device")
    .exceptAll(
        df_sessions
        .filter(F.hour("started_at").between(7, 15))
        .select("id", "device")
    )
)
