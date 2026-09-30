from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.hour("started_at").between(6, 12))
    .select("country")
    .unionByName(
        df_sessions
        .filter(~F.col("browser").isin(["Chrome", "Safari"]))
        .select("country")
    )
    .distinct()
)
