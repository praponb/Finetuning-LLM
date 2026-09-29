from pyspark.sql import functions as F

src = df_payments.filter(F.hour("paid_at").between(7, 10) | (F.col("order_id") == 38))

payments_stats = (
    src
    .groupBy("method")
    .agg(
        F.sort_array(F.collect_set("method")).alias("method_list"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)

result = (
    payments_stats
    .orderBy("method", F.col("amount_range").desc())
    .limit(10)
)
