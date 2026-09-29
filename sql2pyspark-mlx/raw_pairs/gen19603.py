from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("YEAR", "created_at"), "attr_key").orderBy("created_at_year")
w_ordered = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)

flattened = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "value"),
    )
)

exploded = (
    flattened
    .select("id", "created_at", "attr_key", F.explode("coupon_codes").alias("code"))
)

cleaned = (
    exploded
    .filter(F.col("created_at") > F.lit("2025-03-02").cast("date"))
    .select(
        "*",
        F.year("created_at").alias("created_at_year"),
        F.col("created_at").cast("date").alias("created_at_day"),
        F.coalesce("code", F.lit("NONE")).alias("resolved_code"),
    )
)

metrics = (
    cleaned
    .select(
        "created_at",
        "attr_key",
        "created_at_day",
        "resolved_code",
        F.rank().over(w).alias("created_at_year_rank"),
        F.sum("created_at_year").over(w_ordered).alias("created_at_year_sum_1h"),
    )
)

valid = (
    metrics
    .filter(F.col("attr_key").isNotNull())
    .select(
        "created_at",
        "attr_key",
        "created_at_day",
        "created_at_year_rank",
        "created_at_year_sum_1h",
        F.weekofyear("created_at_day").alias("created_at_day_weekofyear"),
        F.coalesce("attr_key", F.lit("OTHER")).alias("attr_key_filled"),
    )
)

result = (
    valid
    .select(
        "created_at",
        "created_at_day",
        "created_at_year_rank",
        "created_at_year_sum_1h",
        "created_at_day_weekofyear",
        "attr_key_filled",
    )
    .orderBy(F.col("created_at").desc())
)
