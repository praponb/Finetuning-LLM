from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("bundle_events").desc())

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "value"),
    )
)

lines_carts = lines.select("id", "k", F.explode("coupon_codes").alias("element"))

base = (
    lines_carts
    .filter(F.col("k").isNotNull())
    .select("*", F.lower("k").alias("k_lower"))
)

valid = (
    base
    .filter(F.col("k").isNotNull())
    .select("*", F.lower("element").alias("element_lower"))
)

filtered = (
    valid
    .filter(F.col("element") == "SPRING10")
    .select("id", "k", "element", F.substring("k", 1, 2).alias("k_prefix"))
)

base_carts = (
    filtered
    .filter(F.col("k").isNotNull())
    .withColumn("k_group", F.when(F.col("k").isin(["device", "gift"]), "Group A").otherwise("Other"))
    .withColumn("k_filled", F.coalesce("k", F.lit("UNKNOWN")))
)

base_carts_2 = (
    base_carts
    .filter(F.col("element").isNotNull() & F.col("k").isin(["utm_source", "weight_kg", "gift"]))
    .select(
        "id",
        "k",
        "element",
        "k_prefix",
        "k_group",
        "k_filled",
        F.lower("k").alias("k_lower"),
    )
)

stats = (
    base_carts_2
    .groupBy("k", "element")
    .agg(
        F.countDistinct("element").alias("distinct_elements"),
        F.count_if(F.col("element") == "BUNDLE").alias("bundle_events"),
        F.sort_array(F.collect_set("element")).alias("element_list"),
    )
    .filter(F.col("distinct_elements") < 1)
    .withColumn("bundle_events_dense_rank", F.dense_rank().over(w))
)

result = (
    stats
    .select(
        "k",
        "element",
        "distinct_elements",
        "bundle_events_dense_rank",
        F.when(F.col("k").rlike("^(weight_kg|gift)$"), "EXTENDED").otherwise("GROUP_A").alias("k_zone"),
    )
)
