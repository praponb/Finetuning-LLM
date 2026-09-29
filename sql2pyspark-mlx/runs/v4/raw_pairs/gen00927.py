from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("element"),
    )
)

cleaned = (
    flattened
    .filter((F.col("created_at") >= F.lit("2025-03-02").cast("date")) & F.col("customer_id").isNotNull())
    .withColumn("element_lower", F.lower("element"))
    .withColumn("element_filled", F.coalesce("element", F.lit("N/A")))
)

kept_keys = (
    cleaned
    .select("customer_id", "id")
    .subtract(
        df_carts
        .filter(F.hour("created_at").between(9, 13))
        .select("customer_id", "id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.name"))
)

valid = (
    with_dims
    .filter(F.col("created_at") > F.lit("2025-03-02").cast("date"))
    .withColumn("name_length", F.length("name"))
    .withColumn("element_upper", F.upper("element"))
    .withColumn("element_filled_2", F.coalesce("element", F.lit("N/A")))
)

enriched = (
    valid
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.name").alias("customer_name"), F.col("b.country"))
)

valid_carts = (
    enriched
    .filter(F.col("created_at") > F.lit("2025-03-02 12:00:00").cast("timestamp"))
    .select(
        "customer_id",
        "created_at",
        "element",
        "element_lower",
        "element_filled",
        "name",
        "name_length",
        "element_upper",
        "element_filled_2",
        "country",
        F.lower("element").alias("element_lower_2"),
        F.col("name_length").cast("double").alias("name_length_num"),
        F.coalesce("element", F.lit("DIRECT")).alias("element_clean"),
    )
)

result = (
    valid_carts
    .withColumn("name_length_tier", F.when(F.col("name_length") >= 66, "LARGE").otherwise("SMALL"))
    .rollup("customer_id", "name_length_tier")
    .agg(
        F.countDistinct("country").alias("distinct_countries"),
        F.count(F.when(F.col("element") == "SPRING10", 1)).alias("spring10_count"),
        F.grouping("customer_id").alias("grouping_level"),
    )
    .filter(F.col("distinct_countries") > 1)
)
