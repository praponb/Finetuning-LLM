from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("cost", "id")
w2 = Window.partitionBy("quarter_start").orderBy(F.col("cost_p95").desc())

deduped = (
    df_shipments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .groupBy(
        F.date_trunc("QUARTER", "delivered_date").alias("quarter_start"),
        F.date_trunc("MONTH", "delivered_date").alias("activity_month"),
    )
    .agg(
        F.percentile_approx("cost", 0.95).alias("cost_p95"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
    .withColumn("cost_p95_rank", F.rank().over(w2))
)
