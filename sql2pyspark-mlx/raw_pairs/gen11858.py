from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy(F.date_trunc("YEAR", "started_at")).orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.date_trunc("month", "started_at"), "browser").orderBy(F.col("started_at").desc(), "id")
w4 = Window.partitionBy(F.to_date("started_at")).orderBy("started_at", "id")

base = (
    df_sessions
    .filter(F.col("customer_id").isNotNull() & (F.col("started_at") > F.to_timestamp(F.lit("2023-05-06 19:00:00"))))
    .select(
        "*",
        F.when(F.col("browser").isin(["Safari", "Firefox"]), "Core").otherwise("Other").alias("browser_group"),
        F.coalesce("country", F.lit("NONE")).alias("country_clean"),
    )
)

with_windows = (
    base
    .filter(F.hour("started_at").between(10, 14))
    .select(
        "id",
        "customer_id",
        "device",
        "browser_group",
        F.row_number().over(w_running).alias("row_seq"),
        F.last("country", ignorenulls=True).over(w2).alias("last_known_country"),
        F.row_number().over(w3).alias("row_seq_2"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

enriched = (
    with_windows
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.city"), F.col("d.credit_limit"), F.col("d.segment"))
)

joined = (
    enriched
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.device",
        "e.row_seq_2",
        "e.segment",
        F.col("c.segment").alias("customer_segment"),
        F.col("c.signup_date"),
    )
)

with_dims = (
    joined
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id").eqNullSafe(F.col("c.id")), "inner")
    .select("e.*", F.col("c.age"))
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("row_seq_2") >= 8, "LARGE").otherwise("SMALL").alias("row_seq_2_tier"),
        "device",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("row_seq_2", 0.95).alias("row_seq_2_p95"),
    )
)
