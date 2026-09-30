from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

base = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-03-17 07:00:00").cast("timestamp")) & (F.col("country").eqNullSafe(F.lit("DE"))))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        F.trim("country").alias("clean_country"),
        F.concat("device", F.lit(" "), "browser").alias("device_browser"),
        F.coalesce("country", F.lit("OTHER")).alias("country_clean"),
    )
)

with_latest = (
    base
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        F.col("c.event_type").alias("web_event_event_type"),
        F.col("c.page"),
    )
)

base_sessions = (
    with_latest
    .filter(F.col("page").isNotNull() & (F.col("page") == "/cart"))
    .withColumn("web_event_event_type_group", F.when(F.col("web_event_event_type").isin(["purchase", "page_view"]), "Group A").otherwise("Other"))
)

eligible = (
    base_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.page") == "/search"), "left_anti")
)

base_sessions_2 = (
    eligible
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.when(F.col("web_event_event_type").isin(["add_to_cart", "purchase"]), "Priority").otherwise("Other").alias("web_event_event_type_group_2"),
    )
)

base_sessions_3 = (
    base_sessions_2
    .filter(F.col("page") == "/checkout")
    .select(
        "id",
        "customer_id",
        "web_event_event_type",
        "page",
        "web_event_event_type_group",
        "web_event_event_type_group_2",
        F.lower("page").alias("page_lower"),
    )
)

eligible_sessions = (
    base_sessions_3
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.page").isNotNull()), "left_anti")
)

result = (
    eligible_sessions
    .select(
        "id",
        "web_event_event_type",
        "page",
        "web_event_event_type_group",
        "web_event_event_type_group_2",
        "page_lower",
    )
    .orderBy(F.col("id").desc_nulls_first(), F.col("web_event_event_type").desc())
)
