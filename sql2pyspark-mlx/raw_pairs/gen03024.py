from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-07-02 17:00:00").cast("timestamp")) & (~F.col("method").eqNullSafe(F.lit("cash"))))
    .select(
        "*",
        F.col("amount").cast("int").alias("amount_int"),
        F.coalesce("method", F.lit("DIRECT")).alias("method_clean"),
    )
)

gaps = (
    base
    .withColumn("session_start_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    islands
    .cube(F.date_trunc("year", "paid_at").alias("year_start"))
    .agg(
        F.max("paid_at").alias("last_paid_at"),
        F.avg("amount_int").alias("avg_amount_int"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.countDistinct("method").alias("distinct_methods"),
        F.round(F.avg("amount"), 2).alias("avg_amount_2"),
    )
)
