from pyspark.sql import functions as F

src = df_subscriptions.filter(F.col("customer_id").isNull())

agg = (
    src
    .groupBy("status")
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
    )
)

result = (
    agg
    .select("status", "plan_list", "cancelled_count")
    .orderBy("status")
    .limit(10)
)
