from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country") == "TH")
    .select("country", "browser")
    .unionByName(
        df_sessions
        .filter(F.col("device").isin(["mobile", "tablet", "desktop"]))
        .select("country", "browser")
    )
)
