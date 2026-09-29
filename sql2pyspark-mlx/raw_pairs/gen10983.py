from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy(F.year("starts_on"), "promo_code").orderBy(F.col("starts_on").desc(), "id")
w3 = Window.partitionBy("promo_code")

valid = (
    df_promotions
    .filter(F.col("channel") != "partner")
    .withColumn("ends_on_label", F.date_format("ends_on", "yyyy-MM"))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("OTHER")))
)

result = (
    valid
    .select(
        "id",
        "channel",
        "starts_on",
        "channel_clean",
        F.sum("discount_pct").over(w).alias("running_discount_pct"),
        F.row_number().over(w_hist).alias("seq_no"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w3)), 4).alias("discount_pct_share"),
    )
)
