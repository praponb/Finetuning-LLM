from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter(F.col("country").isNotNull())
    .select(
        "*",
        F.coalesce("country", F.lit("unassigned")).alias("country_filled"),
        F.when(F.col("browser").isin(["Edge", "Safari"]), "Core").otherwise("Other").alias("browser_group"),
        F.coalesce("country", F.lit("UNKNOWN")).alias("country_clean"),
    )
)

with_gaps = (
    filtered
    .withColumn("new_group_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        "browser_group",
        F.sum("new_group_flag").over(w2).alias("session_no"),
    )
)

result = (
    islands
    .withColumn("quarter_start", F.date_trunc("quarter", "started_at"))
    .groupingSets(
        [["device", "quarter_start", "customer_id"], ["device", "quarter_start"], []],
        "device",
        "quarter_start",
        "customer_id",
    )
    .agg(
        F.round(F.sum("session_no") / F.nullif(F.count(F.when(F.col("browser") == "Edge", 1)), F.lit(0)), 2).alias("session_no_per_edge"),
        F.count("*").alias("event_count"),
        F.percentile_approx("session_no", [0.25, 0.5, 0.75]).alias("session_no_quartiles"),
    )
)
