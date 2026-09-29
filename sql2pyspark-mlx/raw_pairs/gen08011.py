from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("paid_at").desc(), "id")
w2 = Window.partitionBy("order_id")

scored = (
    df_payments
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.stddev_samp("amount").over(w2).alias("order_id_stddev_samp_amount"),
    )
)

result = (
    scored
    .groupBy(F.date_trunc("YEAR", "paid_at").alias("year_start"), "order_id")
    .agg(
        F.size(F.collect_set("method")).alias("num_methods"),
        F.size(F.collect_set("method")).alias("num_methods_2"),
        F.count_if(F.col("status") == "REFUNDED").alias("refunded_events"),
    )
    .orderBy(F.col("num_methods").desc())
)
