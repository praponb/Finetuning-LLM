from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("mobile_events").desc())

result = (
    df_promotions
    .groupBy("promo_code", F.date_trunc("MONTH", "ends_on").alias("activity_month"))
    .agg(
        F.count_if(F.col("channel") == "mobile").alias("mobile_events"),
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
    )
    .withColumn("mobile_events_rank", F.rank().over(w))
)
