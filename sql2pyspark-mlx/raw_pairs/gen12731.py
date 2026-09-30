from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("promo_code").orderBy("promotion_discount_pct", "id", "promo_id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("promotion_promo_code", "promo_code").orderBy("discount_pct", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("promotion_promo_code").orderBy("cumulative_promotion_discount_pct", "id", "promo_id")
w4 = Window.orderBy("promo_id", "id", "promo_id").rowsBetween(-2, -1)
w5 = Window.orderBy("promotion_discount_pct", "id", "promo_id")

attributed = (
    df_promotions
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("a.starts_on") >= F.col("pr.starts_on")) & (F.col("a.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

metrics = (
    attributed
    .filter(F.col("promotion_promo_code") == "VIP")
    .select(
        "id",
        "starts_on",
        "ends_on",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
        F.count("promo_id").over(w_hist).alias("last3_count_promo_id"),
        F.sum("promo_id").over(w_hist).alias("last3_sum_promo_id"),
        F.avg("promo_id").over(w_hist).alias("last3_avg_promo_id"),
        F.sum("promotion_discount_pct").over(w2).alias("cumulative_promotion_discount_pct"),
    )
)

latest = (
    metrics
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("id", "starts_on", "ends_on", "promo_id", "promotion_discount_pct")
)

scored = (
    latest
    .select(
        "id",
        "promo_id",
        "promotion_discount_pct",
        F.avg("promo_id").over(w4).alias("promo_id_avg_2_prior"),
        F.row_number().over(w5).alias("seq_no"),
    )
)

base = (
    scored
    .filter(F.col("promotion_discount_pct") < 0.16)
    .select(
        "*",
        (F.col("seq_no") * F.col("promo_id")).alias("seq_no_times_promo_id"),
        (F.col("promotion_discount_pct") * F.col("promo_id_avg_2_prior")).alias("promotion_discount_pct_times_promo_id_avg_2_prior"),
    )
)

result = (
    base
    .select(
        "id",
        "promo_id",
        "promotion_discount_pct",
        "promo_id_avg_2_prior",
        "seq_no_times_promo_id",
        "promotion_discount_pct_times_promo_id_avg_2_prior",
    )
    .orderBy(
        F.col("promo_id").desc(),
        "promotion_discount_pct_times_promo_id_avg_2_prior",
    )
)
