from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

base = (
    df_sessions
    .filter(F.col("country").isin(["JP", "US", "UK"]) & F.col("browser").isNotNull())
    .select(
        "id",
        "customer_id",
        "browser",
        "country",
        F.trim("browser").alias("clean_browser"),
        F.coalesce("browser", F.lit("N/A")).alias("resolved_browser"),
    )
)

with_dims = (
    base
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id").eqNullSafe(F.col("p.id")), "inner")
    .filter(F.length(F.col("p.email")) >= 11)
    .select(
        "a.id",
        "a.customer_id",
        "a.country",
        "a.clean_browser",
        "a.resolved_browser",
        F.col("p.email"),
        F.col("p.city"),
    )
)

with_dims_sessions = (
    with_dims
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.clean_browser",
        "e.resolved_browser",
        "e.email",
        F.col("c.city").alias("customer_city"),
    )
)

with_last = (
    with_dims_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_type").alias("web_event_event_type"))
)

joined = (
    with_last
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.country"))
)

totals = (
    joined
    .groupBy("customer_city")
    .agg(
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_rows"),
        F.sort_array(F.collect_set("customer_city")).alias("customer_city_list"),
        F.count("*").alias("row_count"),
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("web_event_event_type")).alias("web_event_event_type_list"),
        F.countDistinct("customer_city").alias("distinct_customer_cities"),
    )
)

result = (
    totals
    .select(
        "customer_city",
        "sg_rows",
        "customer_city_list",
        "row_count",
        "web_event_event_type_list",
        "distinct_customer_cities",
        F.when(F.col("customer_city").rlike("^(Chiang Mai|Bangkok)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("customer_city_zone"),
    )
)
