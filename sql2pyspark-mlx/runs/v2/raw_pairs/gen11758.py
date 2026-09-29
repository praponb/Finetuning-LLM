from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") == "/cart")
    .select("id", "page")
    .subtract(
        df_web_events
        .filter(F.col("duration_sec").between(167, 443))
        .select("id", "page")
    )
)
