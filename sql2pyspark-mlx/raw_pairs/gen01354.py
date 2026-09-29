from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("promo_code").orderBy("discount_pct")
w3 = Window.partitionBy("channel", "promo_code").orderBy("discount_pct", "id")

result = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .withColumn("first_known_channel", F.first("channel", ignorenulls=True).over(w))
    .withColumn("discount_pct_cume_dist", F.cume_dist().over(w_ordered))
    .withColumn("prev_discount_pct", F.lag("discount_pct", 2).over(w3))
)
