from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("signup_date").desc(), "id")
w2 = Window.partitionBy("customer_id").orderBy("signup_date", "id")
w3 = Window.partitionBy("country").orderBy("signup_date", "id")
w4 = Window.partitionBy(F.date_trunc("MONTH", "signup_date")).orderBy(F.col("signup_date").desc(), "id")
w_running = Window.partitionBy("customer_id").orderBy("row_seq_2", "id").rowsBetween(-3, Window.currentRow)
w_hist = Window.orderBy("event_seq", "id")

cleaned = (
    df_sessions
    .filter(F.col("browser").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "country",
        F.dayofweek("started_at").alias("started_at_dayofweek"),
        F.coalesce("country", F.lit("NONE")).alias("country_clean"),
    )
)

enriched = (
    cleaned
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .select(
        "s.id",
        "s.customer_id",
        "s.device",
        "s.country",
        "s.country_clean",
        F.col("p.signup_date"),
        F.col("p.name"),
        F.col("p.email"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "customer_id",
        "device",
        "country",
        "email",
        F.row_number().over(w).alias("seq_no"),
        F.row_number().over(w2).alias("event_seq"),
        F.row_number().over(w3).alias("row_seq"),
        F.row_number().over(w4).alias("row_seq_2"),
    )
)

scored_sessions = (
    scored
    .filter(F.col("seq_no").between(4, 6))
    .select(
        "id",
        "device",
        "email",
        "event_seq",
        "row_seq_2",
        F.min("seq_no").over(w_running).alias("last4_min_seq_no"),
        F.max("seq_no").over(w_running).alias("last4_max_seq_no"),
        F.avg("seq_no").over(w_running).alias("last4_avg_seq_no"),
        F.row_number().over(w_hist).alias("row_seq_3"),
    )
)

result = (
    scored_sessions
    .groupBy(
        F.when(F.col("row_seq_3") >= 8, "HIGH").otherwise("LOW").alias("row_seq_3_tier"),
    )
    .agg(
        F.count(F.when(F.col("device") == "tablet", 1)).alias("tablet_rows"),
        F.count("*").alias("num_rows"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("tablet_rows") >= 1)
)
