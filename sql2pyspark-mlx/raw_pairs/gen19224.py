from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())

kept = (
    df_customers
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.started_at") < F.col("s.signup_date")), "left_anti")
)

with_latest = (
    kept
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.city",
        "s.segment",
        "s.is_active",
        F.col("c.browser").alias("session_browser"),
        F.col("c.country").alias("session_country"),
    )
)

eligible = (
    with_latest
    .alias("s")
    .join(df_accounts.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.customer_id").isNull()), "left_semi")
)

with_last = (
    eligible
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.session_browser",
        "s.session_country",
        F.col("c.country").alias("session_country_2"),
        F.col("c.browser"),
    )
)

result = (
    with_last
    .select("id", "name", "session_browser", "session_country", "session_country_2")
    .orderBy(F.col("session_country").desc())
)
