from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("channel", "promo_code").orderBy("discount_pct", "id")

scored = (
    df_promotions
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        F.lag("discount_pct").over(w_grp).alias("prev_discount_pct"),
        F.row_number().over(w_grp).alias("row_seq"),
    )
)

result = (
    scored
    .select("id", "channel", "promo_code", "starts_on", "discount_pct", "row_seq")
    .orderBy("id")
    .limit(10)
)
