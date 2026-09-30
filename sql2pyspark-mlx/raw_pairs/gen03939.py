from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("ends_on", "id")
w_running = Window.partitionBy(F.month("ends_on"), "channel").orderBy("ends_on", "id")
w_ordered = Window.partitionBy("promo_code")

result = (
    df_promotions
    .select(
        "id",
        "channel",
        "promo_code",
        "discount_pct",
        F.row_number().over(w).alias("event_seq"),
        F.lead("discount_pct", 1).over(w_running).alias("next_discount_pct"),
        F.stddev_samp("discount_pct").over(w_ordered).alias("promo_code_stddev_samp_discount_pct"),
    )
)
