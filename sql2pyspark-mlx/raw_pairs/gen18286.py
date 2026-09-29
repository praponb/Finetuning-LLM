from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("started_at", "id")
w_grp = Window.partitionBy("country", "device").orderBy("started_at", "id")
w4 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sessions
    .filter(F.col("country").isNotNull() & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        F.lower("device").alias("device_lower"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.first("country", ignorenulls=True).over(w).alias("first_known_country"),
        F.row_number().over(w2).alias("row_seq"),
        F.row_number().over(w_grp).alias("seq_no"),
    )
)

with_gaps = (
    enriched
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w2).isNull() | (F.col("started_at") > F.lag("started_at").over(w2) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "customer_id",
        "started_at",
        "country",
        "seq_no",
        F.sum("session_start_flag").over(w4).alias("session_id"),
    )
)

matched_keys = (
    sessionized
    .select("customer_id", "id")
    .intersect(
        df_sessions
        .filter(F.to_date("started_at") >= "2025-03-01")
        .select("customer_id", "id")
    )
)

verified = (
    sessionized
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .alias("e")
    .join(df_customers.alias("dim"), F.col("e.customer_id") == F.col("dim.id"), "inner")
    .groupBy(
        F.col("dim.segment").alias("segment"),
        F.col("e.country").alias("session_country"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.col("e.seq_no")).alias("total_seq_no"),
    )
)
