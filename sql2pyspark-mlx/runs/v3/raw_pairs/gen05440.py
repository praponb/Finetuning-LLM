from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~(~F.col("device").isin(["tablet", "mobile", "desktop"]) & F.col("browser").isin(["Chrome", "Edge"])))
    .select(
        "customer_id",
        "country",
        "browser",
        F.dayofmonth("started_at").alias("started_at_day"),
    )
)
