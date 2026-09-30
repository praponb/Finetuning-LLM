from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy(F.col("start_date").desc_nulls_first())

latest = (
    df_subscriptions
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "rank_in_group",
        F.when(F.col("status").rlike("^(ACTIVE|PAUSED)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("status_zone"),
    )
)
