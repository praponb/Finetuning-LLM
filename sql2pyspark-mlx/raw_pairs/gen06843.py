from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~((F.col("started_at") >= "2025-04-12") & F.col("customer_id").isNotNull()))
    .select(
        "id",
        "country",
        "customer_id",
        (F.datediff(F.current_date(), "started_at") / 365.25).alias("years_since_started_at"),
    )
    .orderBy(F.col("years_since_started_at").desc(), F.col("id").desc())
)
