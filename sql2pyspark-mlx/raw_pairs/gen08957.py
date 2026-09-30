from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "status").orderBy(F.col("amount_times_discount").asc_nulls_first(), "id")

valid = (
    df_orders
    .filter(F.col("channel").isNotNull())
    .withColumn("amount_times_discount", (F.col("amount") * F.col("discount")))
    .withColumn("amount_whole", F.floor("amount"))
)

not_flagged = (
    valid
    .alias("s")
    .join(df_payments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.hour(F.col("ch.paid_at")).between(9, 14)), "left_anti")
)

ranked = (
    not_flagged
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "customer_id", "amount", "amount_whole")
)

joined = (
    ranked
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.segment"))
)

result = (
    joined
    .alias("e")
    .join(df_customers.alias("dim"), F.col("e.customer_id") == F.col("dim.id"), "left")
    .groupBy(
        F.col("dim.city").alias("city"),
        F.col("dim.country").alias("country"),
        F.col("e.segment").alias("order_segment"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("e.amount_whole")).alias("min_amount_whole"),
    )
)
