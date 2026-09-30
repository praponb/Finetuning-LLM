from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel")

enriched = (
    df_promotions
    .select(
        "id",
        "promo_code",
        "starts_on",
        "discount_pct",
        F.last("channel", ignorenulls=True).over(w).alias("last_known_channel"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w2)), 4).alias("discount_pct_share"),
    )
)

result = (
    enriched
    .select(
        "id",
        "promo_code",
        "starts_on",
        "discount_pct_share",
        F.when(F.col("promo_code").rlike("^(BUNDLE|WELCOME5)$"), "CORE").when(F.col("promo_code").rlike("^(SPRING10|VIP)$"), "GROUP_B").otherwise("PRIORITY").alias("promo_code_zone"),
    )
    .orderBy(F.col("discount_pct_share").desc())
)
