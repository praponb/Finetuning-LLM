from pyspark.sql import functions as F

cleaned = (
    df_order_items
    .filter(F.col("unit_price") >= 320)
    .select(
        "id",
        "quantity",
        "unit_price",
        F.col("unit_price").cast("int").alias("unit_price_int"),
        F.round("unit_price", 0).alias("unit_price_rounded"),
    )
)

matched_keys = (
    cleaned
    .select("id")
    .intersect(
        df_order_items
        .filter(F.col("unit_price") >= 210)
        .select("id")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("unit_price_int") <= 25)
    .select(
        "id",
        "quantity",
        "unit_price_int",
        "unit_price_rounded",
        (F.col("unit_price_rounded") / F.nullif("unit_price_int", F.lit(0))).alias("unit_price_rounded_per_unit_price_int"),
        (F.col("unit_price_int") + F.col("quantity")).alias("unit_price_int_plus_quantity"),
    )
)

cleaned_order_items = (
    valid
    .filter(F.col("unit_price_rounded").between(25, 64))
    .select(
        "id",
        "quantity",
        "unit_price_rounded",
        "unit_price_int_plus_quantity",
        F.col("unit_price_int").cast("string").alias("unit_price_int_num"),
    )
)

result = (
    cleaned_order_items
    .select(
        "id",
        "quantity",
        "unit_price_rounded",
        "unit_price_int_plus_quantity",
        "unit_price_int_num",
    )
    .orderBy("unit_price_rounded")
)
