from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc())

result = (
    df_subscriptions
    .select("id", "status", "monthly_fee", F.dense_rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 3)
    .select("id", "status", "monthly_fee")
)
