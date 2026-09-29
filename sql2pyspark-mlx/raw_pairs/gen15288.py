from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("starts_on").desc(), "id")

filtered = (
    df_promotions
    .filter(F.col("channel").isNotNull() & (F.col("discount_pct") < 0.2))
    .withColumn("promo_code_length", F.length("promo_code"))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("UNKNOWN")))
)

result = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)
