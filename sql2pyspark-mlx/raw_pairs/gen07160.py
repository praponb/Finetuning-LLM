from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w2 = Window.partitionBy("status", "order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.orderBy("days_since_paid_at", "id")

filtered = (
    df_payments
    .filter(F.col("paid_at") > F.lit("2023-07-21 17:00:00").cast("timestamp"))
    .withColumn("amount_filled", F.coalesce("amount", F.lit(0)))
    .withColumn("days_since_paid_at", F.datediff(F.current_date(), "paid_at"))
)

result = (
    filtered
    .select(
        "id",
        "amount",
        "status",
        "days_since_paid_at",
        F.count("amount").over(w).alias("amount_count_30d"),
        F.max("amount_filled").over(w2).alias("amount_filled_max_7d"),
        F.ntile(4).over(w3).alias("days_since_paid_at_bucket"),
    )
)
