from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("ends_on", "id")
w2 = Window.partitionBy("channel", "promo_code").orderBy(F.col("discount_pct").desc())
w3 = Window.partitionBy("channel").orderBy(F.col("prev_discount_pct").asc_nulls_first(), "id")
w4 = Window.partitionBy("promo_code", "channel").orderBy(F.col("ends_on").desc(), "id")

valid = (
    df_promotions
    .filter(F.col("channel").isNotNull() & (~F.col("channel").eqNullSafe(F.lit("store"))))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.date_format("starts_on", "yyyy-MM-dd").alias("starts_on_label"),
        F.when(F.col("discount_pct") >= 0.33, "High").when(F.col("discount_pct") >= 0.15, "Medium").otherwise("Low").alias("discount_pct_tier"),
    )
)

scored = (
    valid
    .withColumn("prev_discount_pct", F.lag("discount_pct", 1, 0).over(w))
    .withColumn("discount_pct_cume_dist", F.cume_dist().over(w2))
)

deduped = (
    scored
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

deduped_promotions = (
    deduped
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "starts_on", "ends_on", "discount_pct_cume_dist")
)

result = deduped_promotions.select("id", "starts_on", "ends_on")
