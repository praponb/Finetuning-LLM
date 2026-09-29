from pyspark.sql import functions as F

kept_keys = (
    df_promotions
    .select("id", "channel")
    .intersectAll(
        df_promotions
        .filter(F.col("discount_pct") > 0.23)
        .select("id", "channel")
    )
)

kept_rows = (
    df_promotions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("id", "channel", "promo_code", "ends_on", "discount_pct")
    .orderBy(F.col("id").asc_nulls_last())
)
