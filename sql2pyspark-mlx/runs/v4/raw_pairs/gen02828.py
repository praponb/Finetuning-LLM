from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("discount_pct")
w2 = Window.partitionBy("promo_code")
w3 = Window.orderBy(F.col("starts_on").desc(), "id")

result = (
    df_promotions
    .withColumn("discount_pct_dense_rank", F.dense_rank().over(w))
    .withColumn("discount_pct_share", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w2)), 4))
    .withColumn("row_seq", F.row_number().over(w3))
)
