from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country") == "JP")
    .select(
        "browser",
        "customer_id",
        F.length("browser").alias("browser_length"),
        F.date_trunc("week", "started_at").alias("started_at_period"),
    )
    .orderBy("browser")
)
