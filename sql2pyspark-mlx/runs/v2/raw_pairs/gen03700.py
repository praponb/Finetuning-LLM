from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.hour("started_at").between(9, 12))
    .select("customer_id", "country")
    .unionByName(
        df_sessions
        .filter(F.col("customer_id").isNotNull())
        .select("customer_id", "country")
    )
    .orderBy("country")
)
