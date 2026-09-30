from pyspark.sql import functions as F

policy_side = df_policies.select("status", F.col("start_date").alias("event_date"))

policy_side_2 = (
    df_policies
    .filter((F.col("holder_id").isNotNull()) & (F.col("premium") > 1051.9))
    .select(F.col("product").alias("status"), F.col("end_date").alias("event_date"))
)

stacked = policy_side.union(policy_side_2)

result = (
    stacked
    .groupBy("status")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("event_date").alias("latest_event_date"),
    )
    .orderBy("status")
)
