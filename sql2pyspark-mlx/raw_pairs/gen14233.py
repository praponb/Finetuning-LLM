from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "status").orderBy(F.col("monthly_fee").desc_nulls_last(), "id")

ranked = (
    df_subscriptions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "status")
)

result = (
    ranked
    .select("id")
    .orderBy(F.col("id").desc_nulls_first())
)
