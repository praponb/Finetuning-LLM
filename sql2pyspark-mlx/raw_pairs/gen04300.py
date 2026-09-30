from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("promo_code").orderBy("discount_pct", "id")
w2 = Window.partitionBy("channel").orderBy("ends_on", "id")

result = (
    df_promotions
    .filter(F.col("channel") == "mobile")
    .withColumn("prev_discount_pct", F.lag("discount_pct", 1).over(w_recent))
    .withColumn("prev_discount_pct_2", F.lag("discount_pct", 1).over(w2))
)
