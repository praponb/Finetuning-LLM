from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")
w2 = Window.partitionBy("promo_code").orderBy("ends_on", "id")
w3 = Window.partitionBy("channel").orderBy("starts_on", "id")

with_windows = (
    df_promotions
    .select(
        "*",
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w)), 4).alias("discount_pct_share"),
        F.row_number().over(w2).alias("event_seq"),
        F.row_number().over(w3).alias("event_seq_2"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("event_seq") >= 8, "PREMIUM").otherwise("STANDARD").alias("event_seq_tier"),
    )
    .agg(
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
        F.percentile_approx("event_seq_2", 0.9).alias("event_seq_2_p90"),
        F.round(F.avg("discount_pct_share"), 2).alias("avg_discount_pct_share"),
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_rows"),
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_rows"),
    )
    .orderBy(F.col("avg_discount_pct_share").desc())
)
