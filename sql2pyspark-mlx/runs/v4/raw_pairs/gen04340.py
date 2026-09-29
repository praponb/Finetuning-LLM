from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("discount_pct")
w2 = Window.partitionBy("promo_code", "channel").orderBy("ends_on", "id")
w3 = Window.partitionBy("promo_code")
w4 = Window.partitionBy("promo_code").orderBy("starts_on", "id")
w_part = Window.orderBy(F.col("discount_pct").desc())

with_windows = (
    df_promotions
    .withColumn("discount_pct_percent_rank", F.percent_rank().over(w))
    .withColumn("next_discount_pct", F.lead("discount_pct", 2).over(w2))
    .withColumn("promo_code_min_discount_pct", F.min("discount_pct").over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("discount_pct_rank", F.rank().over(w_part))
)

result = (
    with_windows
    .withColumn("year_start", F.date_trunc("YEAR", "ends_on"))
    .rollup("year_start")
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("next_discount_pct"), 2).alias("avg_next_discount_pct"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.sum(F.when(F.col("promo_code") == "WELCOME5", F.col("next_discount_pct")).otherwise(0)).alias("welcome5_next_discount_pct"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("avg_next_discount_pct") < 0)
    .orderBy("row_count")
)
