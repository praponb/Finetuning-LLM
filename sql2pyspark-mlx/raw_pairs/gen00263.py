from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("promo_id_2", "id", "promo_id", "promo_id_2")
w2 = Window.partitionBy("promotion_promo_code").orderBy("promo_id_2", "id", "promo_id", "promo_id_2").rowsBetween(-2, -1)
w3 = Window.partitionBy("channel").orderBy("discount_pct", "id", "promo_id", "promo_id_2")
w4 = Window.partitionBy("promotion_promo_code_2").orderBy("promo_id", "id", "promo_id", "promo_id_2", "promo_id_3")
w5 = Window.partitionBy("promo_code").orderBy(F.col("promo_id_3").asc_nulls_first())
w6 = Window.partitionBy("promo_code").orderBy("discount_pct", "id", "promo_id", "promo_id_2", "promo_id_3")

matched = (
    df_promotions
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.starts_on") >= F.col("pr.starts_on")) & (F.col("s.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

matched_promotions = (
    matched
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & (F.col("a.ends_on") >= F.col("pr.starts_on")) & (F.col("a.ends_on") <= F.date_add(F.col("pr.ends_on"), 10)), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

metrics = (
    matched_promotions
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.avg("promotion_discount_pct_2").over(w2).alias("promotion_discount_pct_2_avg_2_prior"),
    )
)

latest = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "channel",
        "starts_on",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
        "promo_id_2",
        "promotion_promo_code_2",
        "seq_no",
        "promotion_discount_pct_2_avg_2_prior",
    )
)

attributed = (
    latest
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.starts_on") >= F.col("pr.starts_on")) & (F.col("e.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id_3"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

metrics_promotions = (
    attributed
    .filter(F.col("promotion_promo_code_2").isNotNull())
    .select(
        "id",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
        "promo_id_2",
        "promotion_promo_code_2",
        "seq_no",
        "promo_id_3",
        "promo_code",
        F.lead("promo_id_3", 1).over(w4).alias("next_promo_id_3"),
        F.percent_rank().over(w5).alias("promo_id_3_percent_rank"),
        F.row_number().over(w6).alias("row_seq"),
    )
)

remaining_keys = (
    metrics_promotions
    .select("id")
    .subtract(
        df_promotions
        .filter(F.col("channel").isin(["store", "partner"]))
        .select("id")
    )
)

kept_rows = (
    metrics_promotions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        "seq_no",
        "promo_code",
        F.when(F.col("promotion_discount_pct") >= 0.32, "HIGH").otherwise("LOW").alias("promotion_discount_pct_tier"),
    )
    .agg(
        F.sum(F.when(F.col("promotion_promo_code_2") == "WELCOME5", F.col("promo_id_3_percent_rank")).otherwise(0)).alias("welcome5_promo_id_3_percent_rank"),
        F.sort_array(F.collect_set("promotion_promo_code")).alias("promotion_promo_code_list"),
        F.count("*").alias("event_count"),
        F.count("*").alias("event_count_2"),
        F.count(F.when(F.col("promotion_promo_code") == "SPRING10", 1)).alias("spring10_rows"),
    )
    .orderBy(F.col("spring10_rows").desc())
)
