from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~F.col("device").isin(["mobile", "tablet", "desktop"]))
    .select("id", "country")
    .unionByName(
        df_sessions
        .filter(F.col("device") != "mobile")
        .select("id", "country")
    )
    .orderBy(F.col("id").desc())
)
