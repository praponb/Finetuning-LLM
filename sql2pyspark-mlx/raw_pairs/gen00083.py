from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.min("balance").alias("min_account_balance"))

flagged = (
    df_sessions
    .withColumn("is_new_session", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    islands
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        "is_new_session",
        "session_no",
        F.when(F.col("device").rlike("^(mobile|tablet)$"), "OTHER_ZONE").when(F.col("device").rlike("^(desktop)$"), "CORE").otherwise("PRIORITY").alias("device_zone"),
        F.col("min_account_balance"),
    )
    .orderBy(F.col("device_zone").asc_nulls_last(), "session_no")
)
