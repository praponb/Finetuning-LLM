from pyspark.sql import functions as F

result = (
    df_web_events
    .select("id")
    .subtract(
        df_web_events
        .filter(~F.col("page").isin(["/product", "/checkout", "/home"]))
        .select("id")
    )
)
