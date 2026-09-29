from pyspark.sql import functions as F

flattened = df_carts.select("id", F.explode("coupon_codes").alias("element"))

base = (
    flattened
    .filter(F.col("element").isNotNull() & F.col("element").isin(["SPRING10", "BUNDLE"]))
    .select(
        "*",
        F.upper("element").alias("element_upper"),
        F.substring("element", 1, 3).alias("element_prefix"),
    )
)

valid = (
    base
    .filter(F.col("element") == "SPRING10")
    .withColumn("element_group", F.when(F.col("element").isin(["VIP", "SPRING10"]), "Core").otherwise("Other"))
)

base_carts = (
    valid
    .filter(F.col("element").isNotNull() & (~F.col("element").eqNullSafe(F.lit("BUNDLE"))))
    .select(
        "id",
        "element",
        "element_upper",
        "element_prefix",
        "element_group",
        F.initcap(F.trim("element")).alias("element_title"),
        F.lower("element").alias("element_lower"),
    )
)

summary = (
    base_carts
    .groupBy("element")
    .agg(
        F.size(F.collect_set("element")).alias("num_elements"),
        F.countDistinct("element").alias("distinct_elements"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("element") == "VIP", 1)).alias("vip_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("element") == "SPRING10", 1)).alias("spring10_rows"),
    )
)

result = (
    summary
    .select(
        "element",
        "num_elements",
        "distinct_elements",
        "event_count",
        "vip_rows",
        "distinct_ids",
    )
)
