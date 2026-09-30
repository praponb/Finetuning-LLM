from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("ends_on", "id")
w2 = Window.partitionBy("channel").orderBy(F.col("ends_on_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.orderBy(F.col("discount_pct_avg_7d").desc(), "id")
w4 = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("channel", "promo_code").orderBy(F.col("ends_on").desc(), "id")

cleaned = (
    df_promotions
    .filter(F.col("starts_on") >= F.lit("2024-03-28").cast("date"))
    .select(
        "*",
        F.date_trunc("MONTH", "ends_on").alias("ends_on_period"),
        F.coalesce("channel", F.lit("N/A")).alias("resolved_channel"),
    )
)

scored = (
    cleaned
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("discount_pct_avg_7d", F.avg("discount_pct").over(w2))
)

enriched = (
    scored
    .select(
        "*",
        F.ntile(5).over(w3).alias("discount_pct_avg_7d_bucket"),
        F.sum("discount_pct").over(w4).alias("running_discount_pct"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

result = (
    enriched
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        "ends_on_period",
        "resolved_channel",
        "seq_no",
        "discount_pct_avg_7d",
        F.when(F.col("promo_code").rlike("^(SPRING10|WELCOME5)$"), "GROUP_A").when(F.col("promo_code").rlike("^(VIP|BUNDLE)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("promo_code_zone"),
    )
    .orderBy("id")
)
