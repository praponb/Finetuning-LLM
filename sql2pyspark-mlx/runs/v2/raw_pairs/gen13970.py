from pyspark.sql import functions as F

filtered_subscriptions = df_subscriptions.filter(F.col("status") == "ACTIVE")

agg = (
    filtered_subscriptions
    .groupBy("status")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"))
)

result = agg.orderBy(F.col("plan_values").desc(), "status")
