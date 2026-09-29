from pyspark.sql import functions as F

valid = (
    df_subscriptions
    .filter(F.col("end_date") >= F.lit("2023-07-23").cast("date"))
    .withColumn("status_upper", F.upper("status"))
    .withColumn("status_group", F.when(F.col("status").isin(["EXPIRED", "PAUSED"]), "Group A").otherwise("Other"))
)

result = (
    valid
    .select("id", "customer_id")
    .exceptAll(
        df_subscriptions
        .filter(F.col("status") == "PAUSED")
        .select("id", "customer_id")
    )
)
