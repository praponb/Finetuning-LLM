from pyspark.sql import functions as F

exploded = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("code"))

with_dims = (
    exploded
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.customer_id", "e.code", F.col("d.name"))
)

joined = (
    with_dims
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id").eqNullSafe(F.col("d.id")), "left")
    .filter(F.length(F.col("d.name")) < 6)
    .select("e.*", F.col("d.segment"), F.col("d.city"))
)

valid = (
    joined
    .filter(F.col("city").isin(["Singapore", "New York", "Berlin"]))
    .select("name", "segment", F.upper(F.trim("name")).alias("name_normalized"))
)

filtered = (
    valid
    .filter(F.col("segment").isNotNull())
    .withColumn("segment_name_key", F.concat_ws("/", "segment", "name"))
    .withColumn("segment_filled", F.coalesce("segment", F.lit("UNKNOWN")))
)

base = (
    filtered
    .filter(F.col("segment").eqNullSafe(F.lit("Corporate")))
    .select(
        "*",
        F.upper("name").alias("name_upper"),
        F.concat_ws("-", "name", "segment").alias("name_segment_key"),
        F.coalesce("segment", F.lit("OTHER")).alias("resolved_segment"),
    )
)

cleaned = (
    base
    .filter(F.col("segment").eqNullSafe(F.lit("Consumer")))
    .select(
        "name",
        "segment",
        "name_normalized",
        "segment_name_key",
        "name_upper",
        "name_segment_key",
        F.concat_ws("-", "name", "segment").alias("name_segment_key_2"),
        F.initcap(F.trim("segment")).alias("segment_title"),
    )
)

result = (
    cleaned
    .select(
        "name",
        "segment",
        "name_normalized",
        "segment_name_key",
        "name_upper",
        "name_segment_key_2",
        "segment_title",
        F.when(F.col("name").rlike("^(Pim Chaiyaporn|Jack Taylor)$"), "GROUP_B").when(F.col("name").rlike("^(Alice Wong|bob smith)$"), "EXTENDED").otherwise("GROUP_A").alias("name_zone"),
    )
    .orderBy(F.col("segment").desc_nulls_first())
)
