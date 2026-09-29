from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w2 = Window.partitionBy("order_id")
w3 = Window.partitionBy("order_id").orderBy("amount_avg_30d", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("order_id").orderBy("order_id_count_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("amount_avg_30d").desc())
w6 = Window.orderBy("order_id_count_amount", "id")
w7 = Window.partitionBy("method", "order_id").orderBy("amount_avg_30d", "id")

filtered = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-06-11").cast("date")) & F.col("order_id").isNotNull())
    .withColumn("paid_at_period", F.date_trunc("month", "paid_at"))
    .withColumn("clean_method", F.trim("method"))
    .withColumn("method_clean", F.coalesce("method", F.lit("N/A")))
)

metrics = (
    filtered
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "clean_method",
        F.avg("amount").over(w).alias("amount_avg_30d"),
        F.count("amount").over(w2).alias("order_id_count_amount"),
    )
)

valid = (
    metrics
    .filter((F.col("paid_at") >= F.to_date(F.lit("2023-10-05"))) & (F.col("amount") < 420))
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "clean_method",
        "amount_avg_30d",
        "order_id_count_amount",
        (F.datediff(F.current_date(), "paid_at") / 7).alias("weeks_since_paid_at"),
    )
)

metrics_payments = (
    valid
    .withColumn("order_id_count_amount_avg_3_prior", F.avg("order_id_count_amount").over(w3))
    .withColumn("running_amount_avg_30d", F.sum("amount_avg_30d").over(w4))
    .withColumn("amount_avg_30d_dense_rank", F.dense_rank().over(w5))
    .withColumn("order_id_count_amount_bucket", F.ntile(4).over(w6))
    .withColumn("seq_no", F.row_number().over(w7))
)

result = (
    metrics_payments
    .select(
        "id",
        "order_id",
        "paid_at",
        "weeks_since_paid_at",
        "order_id_count_amount_avg_3_prior",
        "running_amount_avg_30d",
        "order_id_count_amount_bucket",
        "seq_no",
    )
    .orderBy(F.col("id").asc_nulls_last())
)
