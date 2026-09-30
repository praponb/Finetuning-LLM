from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy(F.col("monthly_fee").desc(), "id")

ranked = (
    df_subscriptions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .select("id", "customer_id", "plan", "monthly_fee")
    .orderBy(F.col("monthly_fee").desc())
)
