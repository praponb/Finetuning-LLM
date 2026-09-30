from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("created_at")).orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "attr_value"),
    )
)

exploded = (
    lines
    .select(
        "id",
        "created_at",
        "k",
        "attr_value",
        F.explode("coupon_codes").alias("code"),
    )
)

base = (
    exploded
    .filter(F.col("k").isNotNull() & (F.col("k") != "weight_kg"))
    .withColumn("code_k", F.concat("code", F.lit(" "), "k"))
    .withColumn("clean_code", F.trim("code"))
)

cleaned = (
    base
    .filter(F.to_date("created_at") < "2025-02-01")
    .select(
        "id",
        "created_at",
        "code",
        "clean_code",
        F.trim("k").alias("clean_k"),
        F.datediff(F.current_date(), "created_at").alias("days_since_created_at"),
    )
)

metrics = (
    cleaned
    .filter(~F.col("code").isin(["SPRING10", "WELCOME5", "BUNDLE"]))
    .select(
        "id",
        "created_at",
        F.sum("days_since_created_at").over(w).alias("days_since_created_at_sum_30m"),
    )
)

result = (
    metrics
    .withColumn("activity_day", F.date_trunc("DAY", "created_at"))
    .withColumn("days_since_created_at_sum_30m_tier", F.when(F.col("days_since_created_at_sum_30m") >= 338, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["activity_day", "days_since_created_at_sum_30m_tier"], ["activity_day"]],
        "activity_day",
        "days_since_created_at_sum_30m_tier",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("days_since_created_at_sum_30m", 0.95).alias("days_since_created_at_sum_30m_p95"),
        F.percentile_approx("days_since_created_at_sum_30m", [0.25, 0.5, 0.75]).alias("days_since_created_at_sum_30m_quartiles"),
        F.percentile_approx("days_since_created_at_sum_30m", 0.95).alias("days_since_created_at_sum_30m_p95_2"),
    )
    .orderBy(F.col("days_since_created_at_sum_30m_p95_2").desc_nulls_first())
)
