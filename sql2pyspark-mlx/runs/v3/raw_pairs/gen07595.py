from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~F.col("device").isin(["desktop", "tablet", "mobile"]))
    .select("id", "device", "country")
    .unionByName(
        df_sessions
        .filter(~F.col("country").isin(["TH", "JP"]))
        .select("id", "device", "country")
    )
)
