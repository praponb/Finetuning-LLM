from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy("promo_code_length")
w2 = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc(), "id")

valid = (
    df_promotions
    .filter(F.col("starts_on") >= F.lit("2024-04-05").cast("date"))
    .select("*", F.length("promo_code").alias("promo_code_length"))
)

result = (
    valid
    .withColumn("promo_code_length_rank", F.rank().over(w))
    .withColumn("discount_pct_bucket", F.ntile(4).over(w2))
)
