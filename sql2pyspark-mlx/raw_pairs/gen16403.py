from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "plan").orderBy(F.col("start_date").desc())

ranked = (
    df_subscriptions
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    ranked
    .groupBy("plan")
    .agg(
        F.max(F.when(F.col("plan") == "premium", F.col("monthly_fee"))).alias("premium_max_monthly_fee"),
        F.sum(F.when(F.col("plan") == "premium", F.col("monthly_fee")).otherwise(0)).alias("premium_monthly_fee"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)
