from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("signup_date", "id")
w_grp = Window.partitionBy("browser").orderBy(F.col("signup_date").desc(), "id")
w_ordered = Window.partitionBy("device").orderBy(F.col("signup_date").desc(), "id")
w_part = Window.partitionBy("customer_id").orderBy("event_seq_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("customer_id")

base = (
    df_sessions
    .filter((F.col("device") == "desktop") & F.col("customer_id").isNotNull())
    .withColumn("browser_filled", F.coalesce("browser", F.lit("N/A")))
    .withColumn("country_prefix", F.substring("country", 1, 2))
)

joined = (
    base
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.device",
        "e.browser",
        "e.country",
        "e.browser_filled",
        F.col("c.signup_date"),
        F.col("c.email"),
        F.col("c.name"),
    )
)

enriched = (
    joined
    .select(
        "id",
        "customer_id",
        F.row_number().over(w).alias("seq_no"),
        F.row_number().over(w_grp).alias("event_seq"),
        F.row_number().over(w_ordered).alias("event_seq_2"),
    )
)

enriched_sessions = (
    enriched
    .filter(F.col("event_seq") == 8)
    .select(
        "*",
        F.sum("seq_no").over(w_part).alias("running_sum_seq_no"),
        F.avg("seq_no").over(w_part).alias("running_avg_seq_no"),
        F.count("seq_no").over(w_part).alias("running_count_seq_no"),
        F.round((F.col("seq_no") / F.sum("seq_no").over(w5)), 4).alias("seq_no_share"),
    )
)

result = (
    enriched_sessions
    .groupBy("running_count_seq_no")
    .agg(
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
        F.min("event_seq").alias("min_event_seq"),
        F.percentile_approx("seq_no", 0.5).alias("seq_no_p50"),
    )
    .filter(F.col("seq_no_p50") > 0)
)
