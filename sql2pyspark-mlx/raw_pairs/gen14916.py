from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("ends_on")).orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("channel", "promo_code").orderBy("discount_pct", "id").rowsBetween(-2, -1)
w_recent = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.date_trunc("YEAR", "starts_on"), "promo_code").orderBy(F.col("ends_on").desc(), "id")

with_windows = (
    df_promotions
    .select(
        "*",
        F.sum("discount_pct").over(w).alias("running_discount_pct"),
        F.avg("discount_pct").over(w_running).alias("discount_pct_avg_2_prior"),
        F.sum("discount_pct").over(w_recent).alias("cumulative_discount_pct"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "running_discount_pct",
        "discount_pct_avg_2_prior",
        "cumulative_discount_pct",
    )
)
