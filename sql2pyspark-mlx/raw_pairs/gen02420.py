from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter((F.col("we.session_id") == 2) | (F.col("s.device") != "mobile"))
    .select(
        F.col("we.session_id"),
        F.col("s.customer_id"),
        F.col("s.country"),
        F.when(F.col("s.country").isin(["UK", "TH"]), "Group A").otherwise("Other").alias("country_group"),
    )
)
