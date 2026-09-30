from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("promo_code").orderBy("discount_pct")
w2 = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.date_sub("starts_on", 9).alias("starts_on_minus_9d"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_clean"),
    )
)

result = (
    valid
    .filter(F.col("ends_on") < "2025-06-24")
    .withColumn("discount_pct_rank", F.rank().over(w_recent))
    .withColumn("discount_pct_dense_rank", F.dense_rank().over(w_recent))
    .withColumn("cumulative_discount_pct", F.sum("discount_pct").over(w2))
)
