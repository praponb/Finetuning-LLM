from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w2 = Window.partitionBy(F.year("started_at"), "country")
w3 = Window.partitionBy("device", "country").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.partitionBy(F.year("started_at"))
w5 = Window.orderBy(F.col("started_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

valid = (
    df_sessions
    .filter(F.col("browser").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.when(F.col("country") == "SG", 1).otherwise(0).alias("is_sg"),
    )
)

matched_keys = (
    valid
    .select("country")
    .exceptAll(
        df_sessions
        .filter(F.col("browser").isNull())
        .select("country")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

remaining_keys = (
    reconciled
    .select("device", "customer_id")
    .intersect(
        df_sessions
        .filter(F.col("country").isin(["JP", "UK"]))
        .select("device", "customer_id")
    )
)

kept_rows = (
    reconciled
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.device") == F.col("k.device")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .filter(F.col("device") == "mobile")
    .select(
        "*",
        F.count("is_sg").over(w).alias("is_sg_count_30d"),
        F.avg("is_sg").over(w2).alias("started_at_year_avg_is_sg"),
    )
)

metrics_sessions = (
    metrics
    .select(
        "*",
        F.count("is_sg").over(w3).alias("is_sg_count_7d"),
        F.min("is_sg_count_30d").over(w4).alias("started_at_year_min_is_sg_count_30d"),
        F.avg("started_at_year_avg_is_sg").over(w5).alias("started_at_year_avg_is_sg_avg_90d"),
    )
)

result = (
    metrics_sessions
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "inner")
    .rollup(
        F.col("d.segment").alias("segment"),
        F.col("e.country").alias("session_country"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("e.is_sg_count_7d")).alias("total_is_sg_count_7d"),
        F.avg(F.col("e.started_at_year_avg_is_sg")).alias("avg_started_at_year_avg_is_sg"),
        F.grouping_id().alias("level_id"),
    )
)
