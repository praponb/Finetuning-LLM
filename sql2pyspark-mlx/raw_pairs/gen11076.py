from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

cleaned = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-09-02").cast("date")) & F.col("order_id").isNotNull())
    .withColumn("paid_at_hour", F.hour("paid_at"))
    .withColumn("method_clean", F.coalesce("method", F.lit("N/A")))
)

result = (
    cleaned
    .filter(F.col("paid_at") < "2023-11-21")
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        "paid_at_hour",
        F.sum("paid_at_hour").over(w).alias("cumulative_paid_at_hour"),
        F.avg("amount").over(w2).alias("amount_avg_90d"),
        F.max("paid_at_hour").over(w2).alias("paid_at_hour_max_90d"),
    )
)
