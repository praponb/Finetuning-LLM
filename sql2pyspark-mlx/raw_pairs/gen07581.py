from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.hour("started_at").between(8, 12))
    .select("country")
    .union(
        df_accounts
        .filter(F.col("balance").between(40000, 52000))
        .select("status")
    )
    .orderBy("country")
)
