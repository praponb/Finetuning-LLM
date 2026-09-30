from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("ends_on", "id")

latest = (
    df_promotions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .select(
        "id",
        "channel",
        "ends_on",
        "dedup_rank",
        F.when(F.col("channel").rlike("^(mobile|partner)$"), "CORE").otherwise("EXTENDED").alias("channel_zone"),
    )
)
