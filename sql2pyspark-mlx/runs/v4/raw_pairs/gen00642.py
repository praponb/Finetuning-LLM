from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country") == "TH")
    .select("id", "browser", "country")
    .unionByName(
        df_sessions
        .filter(F.col("customer_id") == 16)
        .select("id", "browser", "country")
    )
    .distinct()
    .orderBy("country")
)
