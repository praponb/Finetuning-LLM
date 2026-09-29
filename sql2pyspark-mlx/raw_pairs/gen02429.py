from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("ends_on", "id")
w2 = Window.partitionBy(F.year("starts_on"), "channel").orderBy("ends_on", "id")
w3 = Window.partitionBy("promo_code").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc())
w5 = Window.partitionBy("promo_code").orderBy("row_seq")
w6 = Window.partitionBy("channel", "promo_code").orderBy(F.col("row_seq").desc(), "id")
w7 = Window.orderBy(F.col("prev_discount_pct").desc_nulls_last())
w8 = Window.orderBy("prev_discount_pct")
w9 = Window.orderBy("prev_discount_pct_percent_rank", "id").rowsBetween(-3, Window.currentRow)
w10 = Window.orderBy("row_seq", "id")

metrics = (
    df_promotions
    .filter(F.col("channel").isin(["mobile", "store", "partner"]))
    .select(
        "*",
        F.lag("discount_pct").over(w).alias("prev_discount_pct"),
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

cleaned = (
    metrics
    .filter(F.col("channel").isNotNull() & F.col("prev_discount_pct").between(0.29, 0.3))
    .select(
        "*",
        F.round((F.datediff("ends_on", "starts_on") / 30), 1).alias("months_starts_on_to_ends_on"),
    )
)

enriched = (
    cleaned
    .select(
        "id",
        "promo_code",
        "starts_on",
        "prev_discount_pct",
        "row_seq",
        "months_starts_on_to_ends_on",
        F.first("months_starts_on_to_ends_on", ignorenulls=True).over(w3).alias("first_known_months_starts_on_to_ends_on"),
        F.rank().over(w4).alias("discount_pct_rank"),
        F.dense_rank().over(w5).alias("row_seq_dense_rank"),
        F.row_number().over(w6).alias("event_seq_2"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "starts_on",
        "prev_discount_pct",
        "row_seq",
        "row_seq_dense_rank",
        "event_seq_2",
        F.percent_rank().over(w7).alias("prev_discount_pct_percent_rank"),
        F.rank().over(w8).alias("prev_discount_pct_rank"),
    )
)

scored_promotions = (
    scored
    .select(
        "*",
        F.sum("prev_discount_pct_percent_rank").over(w9).alias("last4_sum_prev_discount_pct_percent_rank"),
        F.min("prev_discount_pct_percent_rank").over(w9).alias("last4_min_prev_discount_pct_percent_rank"),
        F.count("prev_discount_pct_percent_rank").over(w9).alias("last4_count_prev_discount_pct_percent_rank"),
        F.lead("event_seq_2", 2).over(w10).alias("next_event_seq_2"),
    )
)

result = (
    scored_promotions
    .select(
        "id",
        "prev_discount_pct",
        "row_seq",
        "event_seq_2",
        "prev_discount_pct_rank",
        "last4_sum_prev_discount_pct_percent_rank",
        "last4_min_prev_discount_pct_percent_rank",
    )
)
