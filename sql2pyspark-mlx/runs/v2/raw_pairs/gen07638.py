from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(
        F.count(F.when(F.col("plan") == "standard", 1)).alias("standard_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .filter(F.col("standard_count") > 9)
    .select("plan", "standard_count", "status_values")
)
