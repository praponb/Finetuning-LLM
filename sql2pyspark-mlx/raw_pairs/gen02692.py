from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("starts_on", "id")

metrics = (
    df_promotions
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        F.lead("discount_pct").over(w).alias("next_discount_pct"),
        F.row_number().over(w).alias("event_seq"),
    )
)

result = (
    metrics
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        "next_discount_pct",
        F.when(F.col("channel").rlike("^(web|store)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("channel_zone"),
    )
    .orderBy(F.col("starts_on").asc_nulls_last())
)
