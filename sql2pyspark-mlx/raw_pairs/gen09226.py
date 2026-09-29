from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("paid_at").desc(), "id")

ranked = (
    df_payments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "amount", "paid_at")
)

result = (
    ranked
    .groupBy(
        F.date_trunc("quarter", "paid_at").alias("quarter_start"),
        F.date_trunc("month", "paid_at").alias("activity_month"),
    )
    .agg(
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.count("*").alias("row_count"),
        F.count("*").alias("row_count_2"),
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
        F.min("paid_at").alias("started_at"),
        F.max("paid_at").alias("last_paid_at"),
    )
    .orderBy("activity_month")
)
