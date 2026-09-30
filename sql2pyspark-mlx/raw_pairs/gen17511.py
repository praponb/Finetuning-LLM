from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy("qty", "id", "pos")
w2 = Window.partitionBy("promo")
w_grp = Window.orderBy("pos", "id", "pos").rowsBetween(-6, -1)
w_part = Window.partitionBy("promo").orderBy("qty")
w5 = Window.partitionBy("promo").orderBy(F.col("pos_avg_6_prior").desc_nulls_first(), "id", "pos")
w6 = Window.partitionBy("promo").orderBy(F.col("price").desc_nulls_last(), "id", "pos")

flattened = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "line"))
    .select(
        "id",
        "coupon_codes",
        "pos",
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

enriched = (
    flattened
    .select(
        "id",
        "coupon_codes",
        "pos",
        "price",
        "promo",
        F.ntile(3).over(w).alias("qty_bucket"),
        F.max("pos").over(w2).alias("promo_max_pos"),
        F.avg("pos").over(w_grp).alias("pos_avg_6_prior"),
        F.dense_rank().over(w_part).alias("qty_dense_rank"),
    )
)

valid = (
    enriched
    .filter(F.col("promo").isNotNull())
    .select(
        "id",
        "coupon_codes",
        "pos",
        "price",
        "promo",
        "qty_bucket",
        "promo_max_pos",
        "pos_avg_6_prior",
        "qty_dense_rank",
        (F.col("pos_avg_6_prior") / F.nullif("promo_max_pos", F.lit(0))).alias("pos_avg_6_prior_per_promo_max_pos"),
        F.coalesce("promo", F.lit("UNKNOWN")).alias("promo_clean"),
    )
)

ranked = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
)

latest = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    latest
    .select(
        "id",
        "coupon_codes",
        "pos",
        "price",
        "qty_bucket",
        "promo_max_pos",
        "pos_avg_6_prior",
        "qty_dense_rank",
        "promo_clean",
    )
    .orderBy("promo_max_pos")
)
