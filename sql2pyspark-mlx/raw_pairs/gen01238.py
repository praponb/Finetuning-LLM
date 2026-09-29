from pyspark.sql import functions as F

lines = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "rec"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "pos",
        F.col("rec.product_id"),
        F.col("rec.price"),
        F.col("rec.promo"),
    )
)

result = (
    lines
    .groupBy("promo")
    .agg(
        F.count(F.when(F.col("promo") == "VIP", 1)).alias("vip_rows"),
        F.min("created_at").alias("first_created_at"),
        F.count_if(F.col("promo") == "SPRING10").alias("spring10_events"),
        F.count_if(F.col("promo") == "WELCOME5").alias("welcome5_events"),
        F.sum(F.when(F.col("promo") == "SPRING10", F.col("pos")).otherwise(0)).alias("spring10_pos"),
    )
    .filter(F.col("spring10_events") > 1)
    .orderBy(F.col("vip_rows").asc_nulls_first())
)
