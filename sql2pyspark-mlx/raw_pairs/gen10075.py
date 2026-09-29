from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("credit_limit").asc_nulls_first(), "id")

base = (
    df_sessions
    .filter(F.col("started_at") < "2025-09-01")
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.lower("browser").alias("browser_lower"),
    )
)

joined = (
    base
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .filter(F.col("d.age") <= 66)
    .select(
        "s.id",
        "s.customer_id",
        "s.device",
        "s.browser",
        "s.browser_lower",
        F.col("d.credit_limit"),
        F.col("d.age"),
        F.col("d.signup_date"),
    )
)

kept = (
    joined
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") < F.col("s.signup_date")), "left_anti")
)

deduped = (
    kept
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "customer_id",
        "device",
        "browser_lower",
        "credit_limit",
        "age",
        "signup_date",
    )
)

result = deduped.select("id", "age", "signup_date")
