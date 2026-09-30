from pyspark.sql import functions as F

exploded = df_price_tiers.select("id", "currency", F.explode_outer("discounts").alias("val"))

remaining_keys = (
    exploded
    .select("currency", "id")
    .subtract(
        df_price_tiers
        .filter(F.col("plan") == "basic")
        .select("currency", "id")
    )
)

reconciled = (
    exploded
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    reconciled
    .filter(F.col("currency") == "EUR")
    .select("*", F.round("val", 1).alias("val_rounded"))
)

result = (
    cleaned
    .select(
        "id",
        "currency",
        "val_rounded",
        F.when(F.col("currency").rlike("^(EUR|THB)$"), "GROUP_A").when(F.col("currency").rlike("^(USD)$"), "OTHER_ZONE").otherwise("CORE").alias("currency_zone"),
    )
    .orderBy(F.col("id").desc_nulls_first(), "val_rounded")
)
