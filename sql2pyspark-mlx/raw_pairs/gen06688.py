from pyspark.sql import functions as F

lines = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "rec"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "pos",
        F.col("rec.qty"),
        F.col("rec.price"),
        F.col("rec.promo"),
    )
)

lines_carts = (
    lines
    .select(
        "id",
        "created_at",
        "pos",
        "qty",
        "price",
        "promo",
        F.explode("coupon_codes").alias("code"),
    )
)

valid = (
    lines_carts
    .filter((F.col("created_at") >= F.to_timestamp(F.lit("2025-03-01 13:00:00"))) & (F.col("created_at") >= "2024-02-01"))
    .select(
        "id",
        "created_at",
        "pos",
        "qty",
        "price",
        "code",
        F.datediff(F.current_date(), "created_at").alias("days_since_created_at"),
        F.concat_ws("/", "promo", "code").alias("promo_code_key"),
    )
)

valid_carts = (
    valid
    .filter((F.col("created_at") > F.lit("2025-03-01 10:00:00").cast("timestamp")) & (F.col("code").eqNullSafe(F.lit("BUNDLE"))))
    .withColumn("code_group", F.when(F.col("code").isin(["VIP", "WELCOME5"]), "Priority").otherwise("Other"))
    .withColumn("qty_times_days_since_created_at", (F.col("qty") * F.col("days_since_created_at")))
    .withColumn("code_filled", F.coalesce("code", F.lit("DIRECT")))
)

result = (
    valid_carts
    .groupBy(
        F.when(F.col("price") >= 211, "LARGE").otherwise("SMALL").alias("price_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.min("created_at").alias("first_seen"),
    )
)
