from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "method").orderBy(F.col("amount").desc(), "id")

latest = (
    df_payments
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .withColumn("activity_month", F.date_trunc("MONTH", "paid_at"))
    .groupingSets(
        [["method", "activity_month"], ["method"], []],
        "method",
        "activity_month",
    )
    .agg(
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.min("amount").alias("min_amount"),
        F.max("paid_at").alias("last_paid_at"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("min_amount") > 0)
)
