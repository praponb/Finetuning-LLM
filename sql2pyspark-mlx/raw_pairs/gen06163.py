from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code")
w2 = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("promo_code").orderBy("ends_on", "id")
w4 = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(-6, Window.currentRow)
w5 = Window.partitionBy("promo_code", "channel").orderBy("days_starts_on_to_ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_promotions
    .filter(F.col("ends_on") > F.lit("2023-08-09").cast("date"))
    .select(
        "*",
        F.datediff("ends_on", "starts_on").alias("days_starts_on_to_ends_on"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_clean"),
    )
)

result = (
    cleaned
    .filter(F.col("promo_code").isin(["BUNDLE", "SPRING10", "WELCOME5"]))
    .select(
        "*",
        F.min("discount_pct").over(w).alias("promo_code_min_discount_pct"),
        F.avg("days_starts_on_to_ends_on").over(w2).alias("days_starts_on_to_ends_on_avg_6_prior"),
        F.lag("days_starts_on_to_ends_on", 1).over(w3).alias("prev_days_starts_on_to_ends_on"),
        F.avg("days_starts_on_to_ends_on").over(w4).alias("days_starts_on_to_ends_on_avg_6"),
        F.sum("discount_pct").over(w5).alias("cumulative_discount_pct"),
    )
)
