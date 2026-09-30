from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.partitionBy("service").orderBy("is_billing")
w3 = Window.partitionBy("attr_key").orderBy(F.col("is_billing_sum_2h_per_is_billing_percent_rank").desc_nulls_last())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        F.explode("context").alias("attr_key", "value"),
    )
)

matched_keys = (
    lines
    .select("service", "id")
    .subtract(
        df_app_logs
        .filter(F.col("log_ts") >= "2025-06-01")
        .select("service", "id")
    )
)

kept_rows = (
    lines
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.service") == F.col("k.service")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter((F.col("log_ts") >= F.lit("2025-06-01 13:00:00").cast("timestamp")) & (~F.col("attr_key").eqNullSafe(F.lit("region"))))
    .withColumn("is_billing", F.when(F.col("service") == "billing", 1).otherwise(0))
    .withColumn("attr_key_filled", F.coalesce("attr_key", F.lit("OTHER")))
)

enriched = (
    valid
    .select(
        "id",
        "service",
        "host",
        "attr_key",
        "value",
        "is_billing",
        "attr_key_filled",
        F.sum("is_billing").over(w_grp).alias("is_billing_sum_2h"),
        F.percent_rank().over(w2).alias("is_billing_percent_rank"),
    )
)

cleaned = (
    enriched
    .filter(F.col("is_billing") != 72)
    .select(
        "attr_key",
        "value",
        "attr_key_filled",
        "is_billing_percent_rank",
        (F.col("is_billing_sum_2h") / F.nullif("is_billing_percent_rank", F.lit(0))).alias("is_billing_sum_2h_per_is_billing_percent_rank"),
    )
)

metrics = (
    cleaned
    .filter(F.col("is_billing_sum_2h_per_is_billing_percent_rank").isNull())
    .select(
        "*",
        F.cume_dist().over(w3).alias("is_billing_sum_2h_per_is_billing_percent_rank_cume_dist"),
    )
)

stats = (
    metrics
    .withColumn("is_billing_percent_rank_tier", F.when(F.col("is_billing_percent_rank") >= 0.78, "LARGE").otherwise("SMALL"))
    .rollup("is_billing_percent_rank_tier", "attr_key")
    .agg(
        F.count(F.when(F.col("attr_key") == "trace", 1)).alias("trace_rows"),
        F.round(F.avg("is_billing_sum_2h_per_is_billing_percent_rank"), 2).alias("avg_is_billing_sum_2h_per_is_billing_percent_rank"),
        F.percentile_approx("is_billing_sum_2h_per_is_billing_percent_rank", [0.25, 0.5, 0.75]).alias("is_billing_sum_2h_per_is_billing_percent_rank_quartiles"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("trace_rows") >= 1)
)

result = (
    stats
    .select(
        "attr_key",
        "avg_is_billing_sum_2h_per_is_billing_percent_rank",
        "is_billing_sum_2h_per_is_billing_percent_rank_quartiles",
        "grouping_level",
    )
    .orderBy(F.col("attr_key").asc_nulls_first())
)
