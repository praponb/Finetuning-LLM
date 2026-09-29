from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("discount_pct", "id")
w2 = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc(), "id")
w3 = Window.partitionBy("channel").orderBy("starts_on", "id")
w4 = Window.partitionBy("promo_code")
w5 = Window.partitionBy("promo_code").orderBy(F.col("promo_code_avg_discount_pct_whole").desc())
w_running = Window.partitionBy("promo_code", "channel").orderBy("discount_pct_whole", "id")
w7 = Window.partitionBy("promo_code").orderBy("prev_discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_promotions
    .filter(F.col("starts_on") >= F.lit("2023-05-14").cast("date"))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.floor("discount_pct").alias("discount_pct_whole"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.lag("discount_pct", 2).over(w).alias("prev_discount_pct"),
        F.row_number().over(w2).alias("event_seq"),
        F.lead("discount_pct", 1).over(w3).alias("next_discount_pct"),
        F.avg("discount_pct_whole").over(w4).alias("promo_code_avg_discount_pct_whole"),
    )
)

deduped = (
    scored
    .withColumn("row_num", F.rank().over(w5))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

scored_promotions = (
    deduped
    .withColumn("prev_discount_pct_2", F.lag("discount_pct", 1).over(w_running))
    .withColumn("last_known_channel", F.last("channel", ignorenulls=True).over(w7))
)

result = (
    scored_promotions
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "discount_pct_whole",
        "prev_discount_pct",
        "next_discount_pct",
    )
    .orderBy("prev_discount_pct", "channel")
)
