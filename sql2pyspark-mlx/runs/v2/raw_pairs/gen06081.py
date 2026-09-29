from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNull() & F.col("device").isin(["tablet", "mobile", "desktop"]))
    .select(
        "customer_id",
        "country",
        "started_at",
        F.date_trunc("YEAR", "started_at").alias("started_at_period"),
    )
    .orderBy("country")
)
