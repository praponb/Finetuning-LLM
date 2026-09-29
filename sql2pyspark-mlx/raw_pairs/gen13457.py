from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy("ends_on", "id")

deduped = (
    df_promotions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        F.when(F.col("promo_code").rlike("^(VIP|SPRING10)$"), "EXTENDED").otherwise("GROUP_A").alias("promo_code_zone"),
    )
)
