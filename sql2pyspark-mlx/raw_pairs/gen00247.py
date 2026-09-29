from pyspark.sql import functions as F

lines = (
    df_carts
    .select("id", "created_at", "coupon_codes", F.explode("attributes").alias("k", "v"))
)

result = (
    lines
    .select(
        "id",
        "coupon_codes",
        "k",
        "v",
        F.when(F.col("k").rlike("^(gift|utm_source)$"), "OTHER_ZONE").when(F.col("k").rlike("^(device|weight_kg)$"), "GROUP_A").otherwise("PRIORITY").alias("k_zone"),
    )
    .orderBy(F.col("k").desc_nulls_last())
)
