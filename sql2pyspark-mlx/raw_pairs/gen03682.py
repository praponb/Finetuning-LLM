from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy(F.month("started_at")).orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country", "customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-09-22 02:00:00").cast("timestamp")) & F.hour("started_at").between(10, 13))
    .select(
        "*",
        F.when(F.col("device").isin(["mobile", "desktop"]), "Group A").otherwise("Other").alias("device_group"),
    )
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.duration_sec").between(395, 409)), "left_semi")
)

enriched = (
    not_flagged
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.row_number().over(w_hist).alias("row_seq"),
        F.last("country", ignorenulls=True).over(w2).alias("last_known_country"),
        F.last("country", ignorenulls=True).over(w3).alias("last_known_country_2"),
        F.last("country", ignorenulls=True).over(w4).alias("last_known_country_3"),
    )
)

joined = (
    enriched
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.age"), F.col("p.credit_limit"))
)

result = (
    joined
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        "row_seq",
        "last_known_country",
        "last_known_country_2",
        "last_known_country_3",
    )
    .orderBy(F.col("browser").desc(), "row_seq")
)
