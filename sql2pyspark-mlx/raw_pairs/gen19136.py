from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-03-16").cast("date")) & F.col("customer_id").isNull())
    .withColumn("clean_device", F.trim("device"))
)

with_dims = (
    cleaned
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.browser",
        "e.started_at",
        "e.clean_device",
        F.col("p.age"),
        F.col("p.segment"),
    )
)

scored = (
    with_dims
    .select(
        "id",
        "age",
        "segment",
        F.lead("age").over(w).alias("next_age"),
        F.row_number().over(w).alias("event_seq"),
        F.sum("age").over(w2).alias("cumulative_age"),
    )
)

not_flagged = (
    scored
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.session_id").isin([1, 9, 12])), "left_anti")
)

totals = (
    not_flagged
    .groupBy(F.when(F.col("age") >= 71, "LARGE").otherwise("SMALL").alias("age_tier"))
    .agg(
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_count"),
        F.round(F.sum("cumulative_age") / F.nullif(F.count(F.when(F.col("segment") == "SMB", 1)), F.lit(0)), 2).alias("cumulative_age_per_smb"),
        F.round(F.sum("cumulative_age") / F.nullif(F.count(F.when(F.col("segment") == "SMB", 1)), F.lit(0)), 2).alias("cumulative_age_per_smb_2"),
        F.max("event_seq").alias("max_event_seq"),
    )
)

result = (
    totals
    .select(
        "age_tier",
        "cumulative_age_per_smb",
        "cumulative_age_per_smb_2",
        "max_event_seq",
    )
    .orderBy(F.col("age_tier").desc())
)
