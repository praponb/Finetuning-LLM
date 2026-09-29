from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("week", "ends_on"), "channel").orderBy(F.col("discount_pct").desc())
w2 = Window.orderBy("discount_pct", "id")
w3 = Window.partitionBy("promo_code").orderBy("discount_pct", "id")
w4 = Window.partitionBy("promo_code", "channel").orderBy(F.col("next_discount_pct").desc(), "id")
w5 = Window.partitionBy("promo_code").orderBy("starts_on")

enriched = (
    df_promotions
    .withColumn("discount_pct_rank", F.rank().over(w))
    .withColumn("next_discount_pct", F.lead("discount_pct", 1, 0).over(w2))
    .withColumn("discount_pct_bucket", F.ntile(10).over(w3))
)

latest = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "promo_code", "starts_on", "discount_pct")
)

cleaned = (
    latest
    .filter(F.col("starts_on") > F.to_date(F.lit("2023-06-12")))
    .withColumn("promo_code_lower", F.lower("promo_code"))
    .withColumn("starts_on_quarter", F.quarter("starts_on"))
)

filtered = (
    cleaned
    .filter(F.year("starts_on") == 2025)
    .select(
        "id",
        "promo_code",
        "starts_on",
        "discount_pct",
        "promo_code_lower",
        "starts_on_quarter",
        F.date_format("starts_on", "MMM yyyy").alias("starts_on_label"),
    )
)

deduped = (
    filtered
    .withColumn("latest_rank", F.rank().over(w5))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .withColumn("year_start", F.date_trunc("YEAR", "starts_on"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "starts_on"))
    .groupingSets(
        [["promo_code", "year_start", "quarter_start"], ["promo_code", "year_start"], []],
        "promo_code",
        "year_start",
        "quarter_start",
    )
    .agg(
        F.avg(F.when(F.col("promo_code") == "SPRING10", F.col("discount_pct"))).alias("spring10_avg_discount_pct"),
        F.count(F.when(F.col("promo_code") == "SPRING10", 1)).alias("spring10_rows"),
        F.sum(F.when(F.col("promo_code") == "WELCOME5", F.col("discount_pct")).otherwise(0)).alias("welcome5_discount_pct"),
        F.count(F.when(F.col("promo_code") == "BUNDLE", 1)).alias("bundle_rows"),
        F.count(F.when(F.col("promo_code") == "VIP", 1)).alias("vip_count"),
    )
)
