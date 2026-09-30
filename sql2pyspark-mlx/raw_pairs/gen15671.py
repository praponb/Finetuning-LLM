from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")
w_ordered = Window.orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("discount_pct", "id")

scored = (
    df_promotions
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        F.stddev_samp("discount_pct").over(w).alias("channel_stddev_samp_discount_pct"),
        F.sum("discount_pct").over(w_ordered).alias("running_discount_pct"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

result = (
    scored
    .select(
        "id",
        "promo_code",
        "ends_on",
        "running_discount_pct",
        F.when(F.col("promo_code").rlike("^(BUNDLE|SPRING10)$"), "GROUP_B").when(F.col("promo_code").rlike("^(VIP|WELCOME5)$"), "PRIORITY").otherwise("GROUP_A").alias("promo_code_zone"),
    )
)
