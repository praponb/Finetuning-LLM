from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("browser").orderBy("age", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("device")
w_ordered = Window.partitionBy("customer_country").orderBy(F.col("age").desc_nulls_last())
w5 = Window.partitionBy("page")
w6 = Window.orderBy(F.col("age").asc_nulls_first())

with_dims = (
    df_sessions
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.name"), F.col("d.age"))
)

scored = (
    with_dims
    .select(
        "*",
        F.lead("age").over(w_recent).alias("next_age"),
        F.row_number().over(w_recent).alias("row_seq"),
    )
)

with_latest = (
    scored
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.device",
        "s.browser",
        "s.country",
        "s.name",
        "s.age",
        "s.next_age",
        F.col("c.page"),
        F.col("c.duration_sec"),
    )
)

joined = (
    with_latest
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.email"), F.col("p.country").alias("customer_country"))
)

enriched = (
    joined
    .withColumn("age_share", F.round((F.col("age") / F.sum("age").over(w3)), 4))
    .withColumn("age_dense_rank", F.dense_rank().over(w_ordered))
)

kept = (
    enriched
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.duration_sec") >= 286), "left_anti")
)

enriched_sessions = (
    kept
    .select(
        "id",
        "device",
        "browser",
        "country",
        "name",
        "age_share",
        F.min("age_dense_rank").over(w5).alias("page_min_age_dense_rank"),
        F.cume_dist().over(w6).alias("age_cume_dist"),
    )
)

result = (
    enriched_sessions
    .groupBy("browser")
    .agg(
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.percentile_approx("age_cume_dist", 0.5).alias("age_cume_dist_p50"),
        F.count(F.when(F.col("device") == "tablet", 1)).alias("tablet_count"),
        F.size(F.collect_set("device")).alias("num_devices"),
        F.size(F.collect_set("browser")).alias("num_browsers"),
    )
    .orderBy(F.col("tablet_count").asc_nulls_first())
)
