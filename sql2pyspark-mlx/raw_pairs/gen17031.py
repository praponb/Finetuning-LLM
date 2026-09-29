from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "attr_value"),
    )
)

flattened = (
    lines
    .select(
        "id",
        "created_at",
        "attr_key",
        F.explode_outer("coupon_codes").alias("value"),
    )
)

filtered = (
    flattened
    .filter((F.col("created_at") > F.lit("2025-03-02").cast("date")) & F.col("attr_key").isNotNull())
    .withColumn("attr_key_group", F.when(F.col("attr_key").isin(["utm_source", "device"]), "Tier 1").otherwise("Other"))
)

base = (
    filtered
    .filter(F.col("value").isNotNull())
    .select("*", F.upper("value").alias("value_upper"))
)

base_carts = (
    base
    .filter(F.col("created_at") < "2023-08-01")
    .select(
        "id",
        "created_at",
        "attr_key",
        "value",
        F.coalesce("value", F.lit("unassigned")).alias("value_filled"),
        F.coalesce("value", F.lit("NONE")).alias("value_filled_2"),
    )
)

result = base_carts.select("id", "created_at", "value", "value_filled", "value_filled_2")
