from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy(F.col("ends_on").desc(), "id")

deduped = (
    df_promotions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .select("id", "channel", "starts_on", "discount_pct", "rn")
    .orderBy(F.col("discount_pct").desc_nulls_last())
)
