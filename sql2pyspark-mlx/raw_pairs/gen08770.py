from pyspark.sql import functions as F

flattened = df_carts.select("id", "created_at", F.explode("coupon_codes").alias("tag"))

remaining_keys = (
    flattened
    .select("id")
    .intersect(
        df_carts
        .filter(F.col("customer_id").isin([22, 24, 35]))
        .select("id")
    )
)

kept_rows = (
    flattened
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(F.col("created_at") >= F.lit("2025-03-01").cast("date"))
    .withColumn("clean_tag", F.trim("tag"))
    .withColumn("tag_clean", F.coalesce("tag", F.lit("NONE")))
)

base = (
    valid
    .filter(F.col("created_at") >= F.lit("2025-03-01").cast("date"))
    .select(
        "id",
        "created_at",
        "clean_tag",
        "tag_clean",
        F.initcap(F.trim("tag")).alias("tag_title"),
        F.when(F.col("tag").isin(["SPRING10", "VIP"]), "Priority").otherwise("Other").alias("tag_group"),
        F.coalesce("tag", F.lit("OTHER")).alias("tag_clean_2"),
    )
)

base_carts = (
    base
    .filter(F.col("created_at") < "2024-04-01")
    .withColumn("months_since_created_at", (F.datediff(F.current_date(), "created_at") / 30))
)

result = (
    base_carts
    .select("id", "created_at", "tag_title", "months_since_created_at")
    .orderBy(F.col("id").desc_nulls_last(), "months_since_created_at")
)
