from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("ends_on").desc(), "id")
w2 = Window.partitionBy("channel")

latest = (
    df_promotions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

remaining_keys = (
    latest
    .select("promo_code", "id")
    .intersectAll(
        df_promotions
        .filter(F.col("discount_pct") > 0.13)
        .select("promo_code", "id")
    )
)

verified = (
    latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    verified
    .select(
        "promo_code",
        "starts_on",
        "row_num",
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w2)), 4).alias("discount_pct_share"),
    )
)

filtered = (
    scored
    .filter(F.col("starts_on") >= F.to_date(F.lit("2023-09-07")))
    .select(
        "*",
        (F.col("row_num") - F.col("discount_pct_share")).alias("row_num_minus_discount_pct_share"),
    )
)

result = (
    filtered
    .select("promo_code", "discount_pct_share", "row_num_minus_discount_pct_share")
)
