from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("country") != "UK") | (F.col("browser") == "Firefox"))
    .select(
        "id",
        "customer_id",
        "country",
        "browser",
        F.date_trunc("month", "started_at").alias("started_at_period"),
        F.upper("browser").alias("browser_upper"),
    )
)
