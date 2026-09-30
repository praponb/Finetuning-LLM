from pyspark.sql import functions as F

flattened = df_carts.select("id", F.explode_outer("attributes").alias("k", "v"))

filtered = (
    flattened
    .filter(F.col("k").eqNullSafe(F.lit("gift")))
    .withColumn("k_lower", F.lower("k"))
)

valid = (
    filtered
    .filter(F.col("k").isNotNull() & (F.col("k") == "gift"))
    .select(
        "*",
        F.substring("k", 1, 4).alias("k_prefix"),
        F.coalesce("k", F.lit("NONE")).alias("k_filled"),
    )
)

filtered_carts = (
    valid
    .filter(F.col("k") == "gift")
    .select(
        "id",
        "k",
        "k_lower",
        "k_filled",
        F.initcap(F.trim("k")).alias("k_title"),
        F.lower("k").alias("k_lower_2"),
    )
)

result = (
    filtered_carts
    .select(
        "id",
        "k",
        "k_title",
        F.when(F.col("k").rlike("^(device|utm_source)$"), "CORE").otherwise("EXTENDED").alias("k_zone"),
    )
    .orderBy(F.col("k_zone").desc_nulls_first())
)
