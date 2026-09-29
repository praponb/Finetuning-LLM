from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.partitionBy("code").orderBy(F.col("created_at_weekofyear").desc_nulls_first())
w3 = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w_ordered = Window.partitionBy(F.month("created_at"), "customer_id")
w5 = Window.partitionBy("customer_id")
w6 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("code"),
    )
)

kept_keys = (
    flattened
    .select("id")
    .exceptAll(
        df_carts
        .filter(F.array_contains("coupon_codes", "WELCOME5"))
        .select("id")
    )
)

kept_rows = (
    flattened
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("created_at") >= F.to_timestamp(F.lit("2025-03-01 19:00:00")))
    .withColumn("created_at_label", F.date_format("created_at", "MMM yyyy"))
    .withColumn("created_at_weekofyear", F.weekofyear("created_at"))
)

with_windows = (
    base
    .filter(F.col("customer_id") == 7)
    .withColumn("created_at_weekofyear_avg_30m", F.avg("created_at_weekofyear").over(w))
)

enriched = (
    with_windows
    .select(
        "*",
        F.cume_dist().over(w2).alias("created_at_weekofyear_cume_dist"),
        F.sum("created_at_weekofyear").over(w3).alias("created_at_weekofyear_sum_15m"),
        F.min("created_at_weekofyear_avg_30m").over(w_ordered).alias("created_at_month_min_created_at_weekofyear_avg_30m"),
        F.stddev_samp("created_at_weekofyear").over(w5).alias("customer_id_stddev_samp_created_at_weekofyear"),
        F.count("created_at_weekofyear").over(w6).alias("created_at_weekofyear_count_2h"),
    )
)

valid = (
    enriched
    .filter((F.col("created_at") >= F.to_timestamp(F.lit("2025-03-01 17:00:00"))) & (~F.col("code").eqNullSafe(F.lit("SPRING10"))))
    .withColumn("created_at_weekofyear_plus_created_at_weekofyear_count_2h", (F.col("created_at_weekofyear") + F.col("created_at_weekofyear_count_2h")))
    .withColumn("code_clean", F.coalesce("code", F.lit("OTHER")))
)

valid_carts = (
    valid
    .filter(F.col("code").isNotNull() & (F.col("created_at") > F.to_date(F.lit("2025-03-01"))))
    .select(
        "*",
        (F.col("created_at_weekofyear_avg_30m") * F.col("created_at_weekofyear_plus_created_at_weekofyear_count_2h")).alias("created_at_weekofyear_avg_30m_times_created_at_weekofyear_plus_created_at_weekofyear_count_2h"),
        F.when(F.col("created_at_weekofyear_cume_dist") >= 0.7, "High").when(F.col("created_at_weekofyear_cume_dist") >= 0.27, "Medium").otherwise("Low").alias("created_at_weekofyear_cume_dist_band"),
    )
)

summary = (
    valid_carts
    .groupBy(
        F.when(F.col("created_at_weekofyear_sum_15m") >= 430, "LARGE").otherwise("SMALL").alias("created_at_weekofyear_sum_15m_tier"),
        F.when(F.col("created_at_weekofyear_cume_dist") >= 0.9, "LARGE").otherwise("SMALL").alias("created_at_weekofyear_cume_dist_tier"),
        "customer_id",
    )
    .agg(
        F.count_if(F.col("code") == "BUNDLE").alias("bundle_events"),
        F.percentile_approx("customer_id_stddev_samp_created_at_weekofyear", [0.25, 0.5, 0.75]).alias("customer_id_stddev_samp_created_at_weekofyear_quartiles"),
        F.min("created_at").alias("first_created_at"),
    )
)

result = (
    summary
    .select(
        "created_at_weekofyear_sum_15m_tier",
        "created_at_weekofyear_cume_dist_tier",
        "customer_id",
        "bundle_events",
    )
)
