from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("starts_on").desc())

ranked = (
    df_promotions
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "promo_code", "starts_on", "ends_on", "discount_pct")
)

result = (
    ranked
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.when(F.col("promo_code").rlike("^(WELCOME5|SPRING10)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("promo_code_zone"),
    )
    .orderBy(F.col("discount_pct").desc_nulls_first())
)
