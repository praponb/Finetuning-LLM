from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("id", "created_at", "coupon_codes", F.explode("attributes").alias("k", "v"))
)

result = (
    flattened
    .select(
        "id",
        "k",
        F.when(F.col("k").rlike("^(utm_source|gift)$"), "EXTENDED").when(F.col("k").rlike("^(device|weight_kg)$"), "GROUP_A").otherwise("GROUP_B").alias("k_zone"),
    )
    .orderBy(F.col("k").desc_nulls_first())
)
