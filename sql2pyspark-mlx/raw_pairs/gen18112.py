from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "v"),
    )
)

filtered = (
    exploded
    .filter(F.col("k").isNotNull() & (F.size("coupon_codes") > 1))
    .select("k", "v", F.coalesce("k", F.lit("unassigned")).alias("k_filled"))
)

base = (
    filtered
    .filter(F.col("k").isin(["device", "weight_kg"]))
    .select(
        "*",
        F.initcap(F.trim("k")).alias("k_title"),
        F.coalesce("k", F.lit("OTHER")).alias("resolved_k"),
    )
)

base_carts = (
    base
    .filter(F.col("k").eqNullSafe(F.lit("gift")))
    .select(
        "k",
        "v",
        "k_filled",
        "k_title",
        "resolved_k",
        F.lower("k").alias("k_lower"),
    )
)

base_carts_2 = (
    base_carts
    .filter(F.col("k").isNotNull())
    .withColumn("k_group", F.when(F.col("k").isin(["utm_source", "gift"]), "Tier 1").otherwise("Other"))
    .withColumn("clean_k", F.trim("k"))
)

cleaned = (
    base_carts_2
    .filter(F.col("k").isNotNull() & F.col("k").isin(["weight_kg", "device"]))
    .withColumn("clean_k_2", F.trim("k"))
    .withColumn("k_lower_2", F.lower("k"))
)

daily = (
    cleaned
    .groupBy("k")
    .agg(
        F.count_if(F.col("k") == "utm_source").alias("utm_source_events"),
        F.size(F.collect_set("k")).alias("num_ks"),
    )
)

result = (
    daily
    .select(
        "k",
        "utm_source_events",
        "num_ks",
        F.when(F.col("k").rlike("^(weight_kg|utm_source)$"), "GROUP_B").when(F.col("k").rlike("^(device|gift)$"), "EXTENDED").otherwise("CORE").alias("k_zone"),
    )
    .orderBy(F.col("num_ks").desc())
)
