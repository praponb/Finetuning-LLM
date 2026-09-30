from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy("ends_on", "id")

deduped = (
    df_promotions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "channel", "starts_on", "ends_on", "discount_pct")
)

result = (
    deduped
    .select("id", "channel", "starts_on", "discount_pct")
    .orderBy(F.col("channel").asc_nulls_last(), "id")
)
