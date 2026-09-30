from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("YEAR", "ends_on"), "channel").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("days_since_starts_on", "id")

filtered = (
    df_promotions
    .filter(F.col("starts_on") > F.lit("2023-04-17").cast("date"))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.datediff(F.current_date(), "starts_on").alias("days_since_starts_on"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_filled"),
    )
)

result = (
    filtered
    .filter(F.col("discount_pct") > 0.15)
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "channel_filled",
        F.first("days_since_starts_on", ignorenulls=True).over(w).alias("first_known_days_since_starts_on"),
        F.ntile(5).over(w_grp).alias("days_since_starts_on_bucket"),
    )
)
