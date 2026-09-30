from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy(F.col("ends_on").desc(), "id")

deduped = (
    df_promotions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        F.when(F.col("promo_code").rlike("^(BUNDLE|SPRING10)$"), "GROUP_B").when(F.col("promo_code").rlike("^(VIP|WELCOME5)$"), "PRIORITY").otherwise("EXTENDED").alias("promo_code_zone"),
    )
)
