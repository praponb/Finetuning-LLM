from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("starts_on").desc(), "id")

latest = (
    df_promotions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "channel", "ends_on", "discount_pct")
)

result = (
    latest
    .select(
        "id",
        "channel",
        "ends_on",
        "discount_pct",
        F.when(F.col("channel").rlike("^(store|partner)$"), "GROUP_A").when(F.col("channel").rlike("^(web|mobile)$"), "EXTENDED").otherwise("CORE").alias("channel_zone"),
    )
    .orderBy(F.col("discount_pct").asc_nulls_first())
)
