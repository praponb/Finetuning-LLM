from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy(F.col("starts_on").desc(), "id")

deduped = (
    df_promotions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .select("id", "starts_on", "ends_on", "row_num")
    .orderBy(F.col("starts_on").desc_nulls_last())
)
