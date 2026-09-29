from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("promo_code_length").desc(), "id")

valid = (
    df_promotions
    .filter(F.col("ends_on") > F.to_date(F.lit("2023-11-02")))
    .withColumn("channel_prefix", F.substring("channel", 1, 4))
    .withColumn("promo_code_length", F.length("promo_code"))
)

result = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)
