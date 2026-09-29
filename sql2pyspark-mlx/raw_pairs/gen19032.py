from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.max("balance").alias("max_account_balance"))

flagged = (
    df_sessions
    .withColumn("is_new_burst", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_sessions = flagged.withColumn("burst_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_sessions
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "is_new_burst",
        "burst_id",
        F.when(F.col("device").rlike("^(tablet|mobile)$"), "OTHER_ZONE").when(F.col("device").rlike("^(desktop)$"), "GROUP_A").otherwise("GROUP_B").alias("device_zone"),
        F.col("max_account_balance"),
    )
    .orderBy(F.col("id").asc_nulls_last(), "device")
)
