from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("is_vip_times_is_vip_whole").asc_nulls_last())

lines = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("code"))

base = (
    lines
    .filter(F.col("customer_id") == 20)
    .withColumn("is_vip", F.when(F.col("code") == "VIP", 1).otherwise(0))
    .withColumn("code_lower", F.lower("code"))
    .withColumn("resolved_code", F.coalesce("code", F.lit("DIRECT")))
)

cleaned = (
    base
    .filter((~F.col("code").eqNullSafe(F.lit("WELCOME5"))) & F.col("code").isNotNull())
    .withColumn("code_filled", F.coalesce("code", F.lit("unassigned")))
)

cleaned_carts = (
    cleaned
    .filter(F.col("code").isNotNull() & F.col("code").isin(["SPRING10", "VIP"]))
    .select(
        "*",
        F.floor("is_vip").alias("is_vip_whole"),
        F.coalesce("code", F.lit("UNKNOWN")).alias("code_clean"),
    )
)

cleaned_carts_2 = (
    cleaned_carts
    .filter(F.col("is_vip_whole").between(38, 53))
    .select(
        "id",
        "customer_id",
        "code",
        "is_vip",
        "code_lower",
        "code_filled",
        "is_vip_whole",
        "code_clean",
        (F.col("is_vip") * F.col("is_vip_whole")).alias("is_vip_times_is_vip_whole"),
        F.when(F.col("is_vip_whole") >= 76, "High").when(F.col("is_vip_whole") >= 35, "Medium").otherwise("Low").alias("is_vip_whole_tier"),
    )
)

with_windows = (
    cleaned_carts_2
    .filter(F.col("code").isin(["VIP", "SPRING10", "BUNDLE"]))
    .select("*", F.dense_rank().over(w).alias("is_vip_times_is_vip_whole_dense_rank"))
)

filtered = (
    with_windows
    .filter(F.col("customer_id").isNotNull() & F.col("code").isin(["WELCOME5", "SPRING10"]))
    .select("*", F.round((F.col("is_vip") * 1.1), 2).alias("is_vip_adjusted"))
)

result = (
    filtered
    .groupBy(
        F.when(F.col("is_vip_whole") >= 69, "LARGE").otherwise("SMALL").alias("is_vip_whole_tier"),
    )
    .agg(
        F.percentile_approx("is_vip_times_is_vip_whole", [0.25, 0.5, 0.75]).alias("is_vip_times_is_vip_whole_quartiles"),
        F.count("*").alias("event_count"),
        F.min("is_vip_times_is_vip_whole").alias("min_is_vip_times_is_vip_whole"),
        F.count_if(F.col("code") == "WELCOME5").alias("welcome5_events"),
    )
    .filter(F.col("welcome5_events") > 1)
    .orderBy(F.col("is_vip_whole_tier").desc())
)
