from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")
w2 = Window.partitionBy(F.date_trunc("MONTH", "starts_on"), "channel").orderBy(F.col("discount_pct").desc())
w3 = Window.partitionBy(F.year("ends_on"), "promo_code").orderBy("ends_on", "id")
w4 = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("channel").orderBy("starts_on", "id")

with_windows = (
    df_promotions
    .select(
        "id",
        "starts_on",
        "discount_pct",
        F.stddev_samp("discount_pct").over(w).alias("channel_stddev_samp_discount_pct"),
        F.rank().over(w2).alias("discount_pct_rank"),
        F.row_number().over(w3).alias("row_seq"),
        F.avg("discount_pct").over(w4).alias("discount_pct_avg_3"),
        F.lag("discount_pct", 2).over(w5).alias("prev_discount_pct"),
    )
)

result = with_windows.select("id", "row_seq")
