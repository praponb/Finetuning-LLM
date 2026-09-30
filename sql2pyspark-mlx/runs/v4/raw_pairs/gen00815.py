from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy("created_at", "id", "pos")

flattened = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "elem"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "pos",
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

valid = (
    flattened
    .filter(F.col("promo").isNotNull())
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "pos",
        "promo",
        F.array_join("coupon_codes", "|").alias("coupon_codes_text"),
        F.coalesce("promo", F.lit("UNKNOWN")).alias("promo_filled"),
    )
)

deduped = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

filtered = (
    deduped
    .filter(F.array_contains("coupon_codes", "VIP"))
    .withColumn("pos_minus_latest_rank", (F.col("pos") - F.col("latest_rank")))
    .withColumn("created_at_day", F.col("created_at").cast("date"))
)

result = (
    filtered
    .groupBy(F.date_trunc("day", "created_at").alias("activity_day"), "promo")
    .agg(
        F.sum(F.when(F.col("promo") == "VIP", F.col("pos"))).alias("vip_total_pos"),
        F.round(F.avg("pos"), 2).alias("avg_pos"),
        F.max(F.when(F.col("promo") == "WELCOME5", F.col("pos"))).alias("welcome5_max_pos"),
        F.avg("pos").alias("avg_pos_2"),
    )
    .orderBy(F.col("welcome5_max_pos").desc_nulls_last())
)
