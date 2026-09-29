from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy("end_date")

latest = (
    df_subscriptions
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    latest
    .select(
        "id",
        "customer_id",
        "plan",
        F.when(F.col("plan").rlike("^(enterprise|standard)$"), "PRIORITY").otherwise("GROUP_B").alias("plan_zone"),
    )
)
