from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc(), "id")

ranked = (
    df_promotions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "channel", "promo_code", "starts_on", "ends_on", "discount_pct")
)

result = ranked.select("id", "promo_code", "starts_on", "ends_on", "discount_pct")
