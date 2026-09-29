from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("discount_pct", "id")

ranked = (
    df_promotions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    ranked
    .select("id", "channel", "starts_on", "ends_on")
    .orderBy(F.col("id").desc())
)
