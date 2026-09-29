from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment")
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("segment").orderBy(F.col("age").desc())
w4 = Window.partitionBy("country").orderBy("credit_limit", "id")
w5 = Window.partitionBy("segment").orderBy("credit_limit", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_customers
    .withColumn("corr_credit_limit_age", F.corr("credit_limit", "age").over(w))
    .withColumn("corr_credit_limit_age_2", F.corr("credit_limit", "age").over(w2))
    .withColumn("age_percent_rank", F.percent_rank().over(w3))
    .withColumn("prev_credit_limit", F.lag("credit_limit", 1).over(w4))
    .withColumn("credit_limit_avg_6", F.avg("credit_limit").over(w5))
)
