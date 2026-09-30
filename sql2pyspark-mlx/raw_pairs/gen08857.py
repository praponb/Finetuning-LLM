from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

filtered = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.dayofweek("started_at").alias("started_at_dayofweek"),
        F.coalesce("country", F.lit("DIRECT")).alias("resolved_country"),
    )
)

valid = (
    filtered
    .filter((~F.col("browser").eqNullSafe(F.lit("Safari"))) & F.col("country").isNotNull())
    .select("*", F.concat("device", F.lit("/"), "country").alias("device_country"))
)

with_last = (
    valid
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.country",
        "s.device_country",
        F.col("c.page").alias("web_event_page"),
        F.col("c.duration_sec").alias("web_event_duration_sec"),
    )
)

with_dims = (
    with_last
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .filter(F.col("d.age").between(40, 63))
    .select(
        "s.id",
        "s.customer_id",
        "s.country",
        "s.device_country",
        "s.web_event_page",
        "s.web_event_duration_sec",
        F.col("d.city"),
        F.col("d.credit_limit"),
    )
)

kept = (
    with_dims
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.duration_sec").between(352, 394)), "left_anti")
)

daily = (
    kept
    .groupBy(
        "customer_id",
        F.when(F.col("web_event_duration_sec") >= 472, "PREMIUM").otherwise("STANDARD").alias("web_event_duration_sec_tier"),
    )
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.size(F.collect_set("city")).alias("num_cities_2"),
        F.min("credit_limit").alias("min_credit_limit"),
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("credit_limit"))).alias("chiang_mai_total_credit_limit"),
    )
    .filter(F.col("min_credit_limit") > 0)
)

result = (
    daily
    .select(
        "customer_id",
        "web_event_duration_sec_tier",
        "num_cities",
        "chiang_mai_total_credit_limit",
    )
    .orderBy(F.col("num_cities").desc(), "chiang_mai_total_credit_limit")
)
