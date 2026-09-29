from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc(), "id")

deduped = (
    df_promotions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .groupBy(
        F.date_trunc("quarter", "ends_on").alias("quarter_start"),
        F.when(F.col("discount_pct") >= 0.34, "HIGH").otherwise("LOW").alias("discount_pct_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
        F.avg(F.when(F.col("channel") == "partner", F.col("discount_pct"))).alias("partner_avg_discount_pct"),
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
        F.avg("discount_pct").alias("avg_discount_pct_2"),
    )
    .orderBy(F.col("quarter_start").desc())
)
