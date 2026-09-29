from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

filtered = (
    df_sessions
    .filter(F.col("country").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        F.datediff(F.current_date(), "started_at").alias("days_since_started_at"),
        F.coalesce("country", F.lit("N/A")).alias("country_clean"),
    )
)

eligible = (
    filtered
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.session_id").isin([16, 31, 33, 37])), "left_anti")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.device",
        "s.browser",
        "s.days_since_started_at",
        "s.country_clean",
        F.col("c.page").alias("web_event_page"),
    )
)

with_dims = (
    latest_match
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.device",
        "src.days_since_started_at",
        F.col("d.name"),
        F.col("d.age"),
        F.col("d.signup_date"),
    )
)

result = (
    with_dims
    .alias("a")
    .join(df_customers.alias("dim"), F.col("a.customer_id") == F.col("dim.id"), "left")
    .cube(F.col("dim.segment").alias("segment"), F.col("a.device").alias("device"))
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.sum(F.col("a.days_since_started_at")), 2).alias("total_days_since_started_at"),
        F.grouping_id().alias("grouping_level"),
    )
)
