from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("created_at_weekofyear").desc_nulls_first())

lines = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("key", "attr_value"),
    )
)

flattened = (
    lines
    .select(
        "id",
        "created_at",
        "key",
        "attr_value",
        F.explode("coupon_codes").alias("tag"),
    )
)

cleaned = (
    flattened
    .filter(F.col("key") != "device")
    .select("created_at", "attr_value", F.initcap(F.trim("key")).alias("key_title"))
)

base = (
    cleaned
    .filter(F.col("created_at") > F.lit("2025-03-01 18:00:00").cast("timestamp"))
    .select("*", F.weekofyear("created_at").alias("created_at_weekofyear"))
)

metrics = base.select("*", F.rank().over(w).alias("created_at_weekofyear_rank"))

result = (
    metrics
    .groupBy(
        F.date_trunc("HOUR", "created_at").alias("hour_window"),
        F.when(F.col("created_at_weekofyear_rank") >= 7, "HIGH").otherwise("LOW").alias("created_at_weekofyear_rank_tier"),
        F.when(F.col("created_at_weekofyear") >= 73, "PREMIUM").otherwise("STANDARD").alias("created_at_weekofyear_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.max("created_at").alias("last_created_at"),
        F.min("created_at").alias("started_at"),
    )
    .orderBy(F.col("row_count").desc_nulls_first())
)
