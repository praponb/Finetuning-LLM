from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("ends_on", "id")
w2 = Window.partitionBy(F.year("ends_on_plus_months"), "promo_code").orderBy("ends_on", "id").rowsBetween(-2, -1)

valid = (
    df_promotions
    .filter(F.col("starts_on") > F.lit("2023-05-16").cast("date"))
    .select("*", F.add_months("ends_on", 12).alias("ends_on_plus_months"))
)

result = (
    valid
    .filter(F.col("ends_on") >= "2024-09-19")
    .select(
        "id",
        "discount_pct",
        "ends_on_plus_months",
        F.lag("discount_pct", 1).over(w).alias("prev_discount_pct"),
        F.avg("discount_pct").over(w2).alias("discount_pct_avg_2_prior"),
    )
)
