from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("promo_code")
w4 = Window.partitionBy("channel").orderBy(F.col("ends_on").desc(), "id")
w5 = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_promotions
    .withColumn("event_seq", F.row_number().over(w_running))
    .withColumn("cumulative_discount_pct", F.sum("discount_pct").over(w2))
    .withColumn("discount_pct_share", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w3)), 4))
    .withColumn("seq_no", F.row_number().over(w4))
    .withColumn("last_known_channel", F.last("channel", ignorenulls=True).over(w5))
)
