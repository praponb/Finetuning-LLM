from pyspark.sql import functions as F

result = (
    df_sessions
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.device",
        "s.browser",
        "s.started_at",
        "s.country",
        F.col("p.credit_limit"),
        F.col("p.name"),
    )
)
