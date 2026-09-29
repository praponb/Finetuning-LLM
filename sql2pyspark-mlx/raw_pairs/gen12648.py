from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "value"),
    )
)

result = (
    lines
    .select(
        "k",
        F.when(F.col("k").rlike("^(device|utm_source)$"), "PRIORITY").when(F.col("k").rlike("^(gift|weight_kg)$"), "EXTENDED").otherwise("CORE").alias("k_zone"),
    )
    .orderBy(F.col("k_zone").desc_nulls_last(), F.col("k").desc())
)
