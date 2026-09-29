from pyspark.sql import functions as F

lines = df_carts.select("id", F.explode("attributes").alias("attr_key", "v"))

base = (
    lines
    .filter(F.col("attr_key") == "utm_source")
    .withColumn("clean_attr_key", F.trim("attr_key"))
)

cleaned = (
    base
    .filter(F.col("attr_key").isNotNull())
    .withColumn("attr_key_prefix", F.substring("attr_key", 1, 2))
)

valid = (
    cleaned
    .filter(F.col("attr_key").isNotNull())
    .select(
        "*",
        F.trim("attr_key").alias("clean_attr_key_2"),
        F.trim("attr_key").alias("clean_attr_key_3"),
    )
)

valid_carts = (
    valid
    .filter(F.col("attr_key").isNotNull() & (F.col("attr_key") == "weight_kg"))
    .select(
        "id",
        "attr_key",
        "attr_key_prefix",
        F.when(F.col("attr_key").isin(["weight_kg", "utm_source"]), "Group A").otherwise("Other").alias("attr_key_group"),
    )
)

valid_carts_2 = (
    valid_carts
    .filter(F.col("attr_key").isNotNull())
    .select(
        "*",
        F.trim("attr_key").alias("clean_attr_key"),
        F.initcap(F.trim("attr_key")).alias("attr_key_title"),
        F.coalesce("attr_key", F.lit("UNKNOWN")).alias("attr_key_filled"),
    )
)

base_carts = (
    valid_carts_2
    .filter(F.col("attr_key").isNotNull() & (F.col("attr_key") != "utm_source"))
    .select(
        "id",
        "attr_key",
        "attr_key_group",
        "clean_attr_key",
        "attr_key_filled",
        F.trim("attr_key").alias("clean_attr_key_2"),
        F.coalesce("attr_key", F.lit("NONE")).alias("attr_key_clean"),
    )
)

stats = (
    base_carts
    .rollup("attr_key")
    .agg(
        F.countDistinct("attr_key").alias("distinct_attr_keys"),
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
        F.size(F.collect_set("attr_key")).alias("num_attr_keys"),
        F.size(F.collect_set("attr_key")).alias("num_attr_keys_2"),
        F.grouping("attr_key").alias("grouping_id"),
    )
)

result = (
    stats
    .select("attr_key", "distinct_attr_keys", "attr_key_list", "grouping_id")
    .orderBy("attr_key")
)
