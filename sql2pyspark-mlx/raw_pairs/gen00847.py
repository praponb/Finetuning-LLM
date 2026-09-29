from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("order_id").orderBy("paid_at", "id")

metrics = (
    df_payments
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        F.lag("amount").over(w_recent).alias("prev_amount"),
        F.row_number().over(w_recent).alias("row_seq"),
    )
)

result = (
    metrics
    .withColumn("row_seq_tier", F.when(F.col("row_seq") >= 7, "LARGE").otherwise("SMALL"))
    .withColumn("year_start", F.date_trunc("year", "paid_at"))
    .groupingSets(
        [["method", "row_seq_tier", "year_start"], ["method", "row_seq_tier"], []],
        "method",
        "row_seq_tier",
        "year_start",
    )
    .agg(
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
        F.max("paid_at").alias("ended_at"),
        F.size(F.collect_set("method")).alias("num_methods"),
        F.grouping("method").alias("grouping_id"),
    )
    .orderBy("row_seq_tier")
)
