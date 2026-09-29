from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser") != "Edge")
    .select("country")
    .unionByName(
        df_sessions
        .filter(F.hour("started_at").between(6, 13))
        .select("country")
    )
)
