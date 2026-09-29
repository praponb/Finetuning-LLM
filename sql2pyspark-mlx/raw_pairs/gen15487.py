from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy("starts_on", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("promo_code")
w3 = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc())
w_ordered = Window.orderBy("promo_code_sum_discount_pct", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.orderBy(F.col("discount_pct").desc(), "id")
w7 = Window.partitionBy(F.year("starts_on"), "channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("discount_pct", "id")
w9 = Window.partitionBy("promo_code").orderBy("promo_code_sum_discount_pct_avg_3", "id").rowsBetween(-6, -1)
w10 = Window.orderBy("promo_code_sum_discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("promo_code").orderBy("promo_code_sum_discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("promo_code").orderBy(F.col("running_discount_pct").asc_nulls_first(), "id")
w13 = Window.partitionBy("promo_code").orderBy("discount_pct_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .select(
        "*",
        F.concat("promo_code", F.lit("/"), "channel").alias("promo_code_channel"),
        F.concat("promo_code", F.lit("/"), "channel").alias("promo_code_channel_2"),
    )
)

enriched = (
    filtered
    .withColumn("discount_pct_avg_6_prior", F.avg("discount_pct").over(w))
    .withColumn("promo_code_sum_discount_pct", F.sum("discount_pct").over(w2))
    .withColumn("first_known_channel", F.first("channel", ignorenulls=True).over(w3))
)

enriched_promotions = (
    enriched
    .select(
        "id",
        "promo_code",
        "discount_pct",
        "promo_code_channel",
        "promo_code_channel_2",
        "promo_code_sum_discount_pct",
        F.percent_rank().over(w_part).alias("discount_pct_percent_rank"),
        F.avg("promo_code_sum_discount_pct").over(w_ordered).alias("promo_code_sum_discount_pct_avg_3"),
        F.row_number().over(w6).alias("row_seq"),
        F.sum("discount_pct").over(w7).alias("cumulative_discount_pct"),
    )
)

metrics = (
    enriched_promotions
    .filter(F.col("discount_pct_percent_rank").isNotNull())
    .withColumn("prev_row_seq", F.lag("row_seq", 1).over(w8))
    .withColumn("cumulative_discount_pct_avg_6_prior", F.avg("cumulative_discount_pct").over(w9))
)

scored = (
    metrics
    .select(
        "id",
        "promo_code",
        "promo_code_channel",
        "promo_code_channel_2",
        "promo_code_sum_discount_pct",
        "discount_pct_percent_rank",
        "promo_code_sum_discount_pct_avg_3",
        "cumulative_discount_pct",
        "cumulative_discount_pct_avg_6_prior",
        F.last("discount_pct_percent_rank", ignorenulls=True).over(w10).alias("last_known_discount_pct_percent_rank"),
        F.round((F.col("prev_row_seq") / F.sum("prev_row_seq").over(w2)), 4).alias("prev_row_seq_share"),
        F.sum("discount_pct").over(w11).alias("running_discount_pct"),
    )
)

deduped = (
    scored
    .withColumn("rn", F.row_number().over(w12))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "promo_code",
        "promo_code_channel_2",
        "promo_code_sum_discount_pct",
        "discount_pct_percent_rank",
        "cumulative_discount_pct",
        "last_known_discount_pct_percent_rank",
        "prev_row_seq_share",
    )
)

scored_promotions = (
    deduped
    .select(
        "*",
        F.sum("cumulative_discount_pct").over(w13).alias("running_sum_cumulative_discount_pct"),
        F.max("cumulative_discount_pct").over(w13).alias("running_max_cumulative_discount_pct"),
        F.avg("cumulative_discount_pct").over(w13).alias("running_avg_cumulative_discount_pct"),
    )
)

result = (
    scored_promotions
    .select(
        "id",
        "promo_code",
        "promo_code_channel_2",
        "promo_code_sum_discount_pct",
        "cumulative_discount_pct",
        "last_known_discount_pct_percent_rank",
        "prev_row_seq_share",
        "running_sum_cumulative_discount_pct",
        "running_max_cumulative_discount_pct",
        F.when(F.col("promo_code").rlike("^(VIP|SPRING10)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("promo_code_zone"),
    )
    .orderBy(F.col("promo_code_zone").desc_nulls_first(), "promo_code_sum_discount_pct")
)
