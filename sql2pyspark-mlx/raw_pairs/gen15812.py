from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("ends_on").desc(), "id")
w2 = Window.partitionBy("channel", "promo_code").orderBy("ends_on", "id")
w3 = Window.partitionBy("channel", "promo_code").orderBy(F.col("starts_on").desc(), "id")
w4 = Window.partitionBy("promo_code").orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("promo_code", "channel").orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

deduped = (
    df_promotions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "channel", "promo_code", "starts_on", "ends_on")
)

latest = (
    deduped
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

ranked = (
    latest
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    ranked
    .select(
        "id",
        "channel",
        "row_num",
        "dedup_rank",
        F.first("channel", ignorenulls=True).over(w4).alias("first_known_channel"),
        F.first("channel", ignorenulls=True).over(w_part).alias("first_known_channel_2"),
    )
)

cleaned = (
    metrics
    .filter(F.col("channel").isNotNull() & (F.col("row_num") == 1))
    .select("*", F.upper("channel").alias("channel_upper"))
)

result = (
    cleaned
    .select("id", "channel", "row_num", "first_known_channel")
    .orderBy(F.col("row_num").desc())
)
