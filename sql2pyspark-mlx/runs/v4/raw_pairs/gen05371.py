from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount_pct")
w2 = Window.orderBy(F.col("discount_pct").desc(), "id")
w3 = Window.partitionBy("channel").orderBy(F.col("ends_on").desc(), "id")
w4 = Window.partitionBy("promo_code")

result = (
    df_promotions
    .withColumn("discount_pct_percent_rank", F.percent_rank().over(w))
    .withColumn("discount_pct_bucket", F.ntile(10).over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("discount_pct_share", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w4)), 4))
)
