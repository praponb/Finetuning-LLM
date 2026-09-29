from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Edge", "Firefox"]) & ((F.col("country") == "DE") | (F.col("customer_id") == 33)))
    .select(
        "browser",
        "country",
        F.coalesce("country", F.lit("unassigned")).alias("country_filled"),
    )
)
