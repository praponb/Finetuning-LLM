from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy("channel")
    .pivot("promo_code", ["SPRING10", "VIP"])
    .agg(F.sum("discount_pct").alias("total"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "channel",
        F.col("SPRING10_total").alias("spring10_total"),
        F.col("SPRING10_cnt").alias("spring10_cnt"),
        F.col("VIP_total").alias("vip_total"),
        F.col("VIP_cnt").alias("vip_cnt"),
    )
)
