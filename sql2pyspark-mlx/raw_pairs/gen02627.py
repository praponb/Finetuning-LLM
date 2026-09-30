from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("starts_on").desc())

result = (
    df_promotions
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)
