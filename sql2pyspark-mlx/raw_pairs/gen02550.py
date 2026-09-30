from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").asc_nulls_last(), "id")

latest = (
    df_payments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .groupBy(F.date_trunc("QUARTER", "paid_at").alias("quarter_start"))
    .agg(
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("event_count") > 1)
)
