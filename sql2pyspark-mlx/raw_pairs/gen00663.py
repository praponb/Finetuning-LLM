from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.partitionBy("promo_code").orderBy("discount_pct")
w3 = Window.partitionBy("promo_code")
w4 = Window.partitionBy("promo_code").orderBy("latest_rank_2", "id").rowsBetween(-6, Window.currentRow)
w5 = Window.orderBy(F.col("discount_pct").desc(), "id")
w6 = Window.partitionBy(F.year("starts_on"), "promo_code").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("promo_code").orderBy(F.col("event_seq").desc())
w8 = Window.partitionBy("promo_code", "channel").orderBy("latest_rank", "id")
w9 = Window.orderBy("discount_pct_int_avg_6", "id")
w10 = Window.partitionBy("promo_code", "channel").orderBy("event_seq_rank", "id").rowsBetween(-2, Window.currentRow)

cleaned = (
    df_promotions
    .filter(F.col("channel").eqNullSafe(F.lit("partner")))
    .withColumn("discount_pct_int", F.col("discount_pct").cast("int"))
)

ranked = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

ranked_promotions = (
    ranked
    .withColumn("latest_rank_2", F.rank().over(w2))
    .filter(F.col("latest_rank_2") == 1)
)

metrics = (
    ranked_promotions
    .filter(F.col("promo_code") != "WELCOME5")
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        "latest_rank",
        "latest_rank_2",
        F.sum("discount_pct_int").over(w3).alias("promo_code_sum_discount_pct_int"),
        F.avg("discount_pct_int").over(w4).alias("discount_pct_int_avg_6"),
        F.row_number().over(w5).alias("event_seq"),
        F.first("channel", ignorenulls=True).over(w6).alias("first_known_channel"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "channel",
        "promo_code",
        "discount_pct",
        "latest_rank_2",
        "promo_code_sum_discount_pct_int",
        "first_known_channel",
        F.rank().over(w7).alias("event_seq_rank"),
        F.lag("discount_pct_int_avg_6", 1).over(w8).alias("prev_discount_pct_int_avg_6"),
        F.row_number().over(w9).alias("seq_no"),
    )
)

scored_promotions = (
    scored
    .select(
        "id",
        "promo_code",
        "discount_pct",
        "latest_rank_2",
        "prev_discount_pct_int_avg_6",
        F.count("prev_discount_pct_int_avg_6").over(w10).alias("last3_count_prev_discount_pct_int_avg_6"),
        F.sum("prev_discount_pct_int_avg_6").over(w10).alias("last3_sum_prev_discount_pct_int_avg_6"),
        F.avg("prev_discount_pct_int_avg_6").over(w10).alias("last3_avg_prev_discount_pct_int_avg_6"),
    )
)

result = (
    scored_promotions
    .select(
        "id",
        "promo_code",
        "last3_avg_prev_discount_pct_int_avg_6",
        "last3_sum_prev_discount_pct_int_avg_6",
        "discount_pct",
    )
    .unpivot(
        ids=["id", "promo_code"],
        values=["last3_avg_prev_discount_pct_int_avg_6", "last3_sum_prev_discount_pct_int_avg_6", "discount_pct"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
