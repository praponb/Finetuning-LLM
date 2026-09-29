from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy("start_date")

ranked = (
    df_subscriptions
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "end_date")
)

result = (
    ranked
    .withColumn("activity_month", F.date_trunc("MONTH", "end_date"))
    .rollup("activity_month")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("distinct_ids_2").desc())
)
