from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~(F.col("customer_id").isNotNull() | F.col("country").isin(["SG", "DE"])))
    .select(
        "id",
        "country",
        "device",
        F.date_format("started_at", "MMM yyyy").alias("started_at_label"),
        F.initcap(F.trim("country")).alias("country_title"),
    )
)
