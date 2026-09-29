from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/product", "/checkout", "/search"]))
    .select("duration_sec")
    .unionByName(
        df_web_events
        .filter(F.col("duration_sec") != 136)
        .select("duration_sec")
    )
)
