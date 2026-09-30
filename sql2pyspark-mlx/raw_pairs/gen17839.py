from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_payments
    .withColumn("is_new_session", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_payments = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    sessions_payments
    .withColumn("is_new_session_tier", F.when(F.col("is_new_session") >= 1, "PREMIUM").otherwise("STANDARD"))
    .rollup("status", "order_id", "is_new_session_tier")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("num_rows_2"),
        F.grouping("status").alias("grouping_id"),
    )
    .filter(F.col("num_rows_2") > 1)
)
