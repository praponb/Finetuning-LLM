from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("ends_on"))
w2 = Window.partitionBy("promo_code", "channel").orderBy("ends_on", "id")

scored = (
    df_promotions
    .select(
        "id",
        "promo_code",
        "starts_on",
        "discount_pct",
        F.count("discount_pct").over(w).alias("ends_on_year_count_discount_pct"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

result = (
    scored
    .select(
        "id",
        "promo_code",
        "starts_on",
        "discount_pct",
        "ends_on_year_count_discount_pct",
        "row_seq",
        F.when(F.col("promo_code").rlike("^(SPRING10|BUNDLE)$"), "OTHER_ZONE").when(F.col("promo_code").rlike("^(VIP|WELCOME5)$"), "GROUP_A").otherwise("EXTENDED").alias("promo_code_zone"),
    )
    .orderBy("starts_on")
)
