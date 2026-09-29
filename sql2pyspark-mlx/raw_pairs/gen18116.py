from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel", "promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_promotions
    .filter((F.col("ends_on") >= F.to_date(F.lit("2023-09-30"))) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        F.date_trunc("WEEK", "ends_on").alias("ends_on_period"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_clean"),
    )
)

result = (
    base
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "ends_on_period",
        "channel_clean",
        F.first("channel", ignorenulls=True).over(w).alias("first_known_channel"),
        F.last("channel", ignorenulls=True).over(w2).alias("last_known_channel"),
    )
)
