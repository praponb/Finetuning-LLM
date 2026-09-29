from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("end_date").desc_nulls_first(), "id")

ranked = (
    df_subscriptions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = ranked.select("id", "customer_id", "plan", "start_date", "dedup_rank")
