from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy(F.col("starts_on").desc(), "id")

filtered = (
    df_promotions
    .filter(F.col("starts_on") >= F.to_date(F.lit("2023-10-12")))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        F.date_add("ends_on", 87).alias("ends_on_plus_87d"),
        F.round("discount_pct", 2).alias("discount_pct_rounded"),
    )
)

result = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)
