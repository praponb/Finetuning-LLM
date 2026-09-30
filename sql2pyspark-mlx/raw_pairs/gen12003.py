from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy(F.col("amount").desc(), "id")
w_grp = Window.orderBy("amount")
w_part = Window.partitionBy(F.to_date("paid_at"), "order_id").orderBy("amount", "id")

with_windows = (
    df_payments
    .withColumn("row_seq", F.row_number().over(w_recent))
    .withColumn("amount_rank", F.rank().over(w_grp))
    .withColumn("amount_bucket", F.ntile(3).over(w_part))
)

result = (
    with_windows
    .groupBy("status", "order_id")
    .agg(
        F.min("row_seq").alias("min_row_seq"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.size(F.collect_set("method")).alias("num_methods"),
    )
    .orderBy(F.col("status").desc())
)
