from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("ends_on_plus_82d", "id")
w2 = Window.orderBy("ends_on", "id")
w3 = Window.partitionBy(F.date_trunc("week", "ends_on")).orderBy("ends_on", "id")
w4 = Window.partitionBy("channel").orderBy(F.col("months_starts_on_to_ends_on").desc(), "id")

base = (
    df_promotions
    .filter((F.col("discount_pct") <= 0.3) & F.col("channel").isNotNull())
    .select(
        "*",
        F.date_add("ends_on", 82).alias("ends_on_plus_82d"),
        F.round((F.datediff("ends_on", "starts_on") / 30), 1).alias("months_starts_on_to_ends_on"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_clean"),
    )
)

result = (
    base
    .select(
        "*",
        F.lead("discount_pct", 1).over(w).alias("next_discount_pct"),
        F.lead("months_starts_on_to_ends_on", 1).over(w2).alias("next_months_starts_on_to_ends_on"),
        F.lag("discount_pct", 1, 0).over(w3).alias("prev_discount_pct"),
        F.row_number().over(w4).alias("row_seq"),
    )
)
