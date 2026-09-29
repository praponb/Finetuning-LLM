from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy("starts_on", "id")
w2 = Window.partitionBy("promo_code")

base = (
    df_promotions
    .filter(F.col("channel").isNotNull() & (F.col("discount_pct") > 0.2))
    .withColumn("starts_on_plus_months", F.add_months("starts_on", 1))
    .withColumn("promo_code_length", F.length("promo_code"))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("OTHER")))
)

result = (
    base
    .select(
        "id",
        "channel",
        "discount_pct",
        "starts_on_plus_months",
        "promo_code_length",
        F.lead("promo_code_length").over(w).alias("next_promo_code_length"),
        F.lag("promo_code_length").over(w).alias("prev_promo_code_length"),
        F.lag("discount_pct").over(w).alias("prev_discount_pct"),
        F.row_number().over(w).alias("seq_no"),
        F.count("discount_pct").over(w2).alias("promo_code_count_discount_pct"),
    )
)
