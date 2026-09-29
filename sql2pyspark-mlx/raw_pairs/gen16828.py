from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())

cleaned = (
    df_customers
    .filter(F.col("segment").isNotNull() & F.col("segment").isin(["SMB", "Consumer", "Corporate"]))
    .select(
        "id",
        "name",
        "email",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.length("country").alias("country_length"),
        (F.col("credit_limit") * F.col("age")).alias("credit_limit_times_age"),
        F.coalesce("segment", F.lit("N/A")).alias("segment_filled"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.segment",
        "s.signup_date",
        "s.credit_limit",
        "s.credit_limit_times_age",
        "s.segment_filled",
        F.col("c.device").alias("session_device"),
        F.col("c.country").alias("session_country"),
    )
)
