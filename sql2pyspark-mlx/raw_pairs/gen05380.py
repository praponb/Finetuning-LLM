from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())

valid = (
    df_customers
    .filter(F.col("signup_date") <= "2025-02-01")
    .select(
        "id",
        "email",
        "country",
        "city",
        "segment",
        "is_active",
        "credit_limit",
        "age",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
        (F.col("credit_limit") * F.col("age")).alias("credit_limit_times_age"),
    )
)

with_last = (
    valid
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.device").alias("session_device"),
        F.col("c.country").alias("session_country"),
    )
)

not_flagged = (
    with_last
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.status") == "RETURNED"), "left_anti")
)

with_last_customers = (
    not_flagged
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.email",
        "s.country",
        "s.is_active",
        "s.is_active_label",
        "s.session_country",
        F.col("c.country").alias("session_country_2"),
    )
)

with_latest = (
    with_last_customers
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.channel"))
)

result = (
    with_latest
    .groupBy("country")
    .agg(
        F.countDistinct("channel").alias("distinct_channels"),
        F.size(F.collect_set("session_country")).alias("num_session_countries"),
        F.count("*").alias("cnt"),
        F.count("*").alias("row_count"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("cnt").desc())
)
