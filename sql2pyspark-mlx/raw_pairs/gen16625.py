from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.sum("balance").alias("sum_account_balance"))

with_gaps = (
    df_sessions
    .withColumn("is_new_session", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_sessions = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    sessions_sessions
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select("id", "customer_id", "started_at", "country", F.col("sum_account_balance"))
)
