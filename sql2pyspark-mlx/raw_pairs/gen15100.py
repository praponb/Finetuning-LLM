from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(-6, Window.currentRow)

cleaned = (
    df_payments
    .filter(F.col("method").isNotNull() & F.col("order_id").isin([5, 11, 17]))
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.when(F.col("amount") >= 1300, "High").when(F.col("amount") >= 670, "Medium").otherwise("Low").alias("amount_level"),
    )
)

result = (
    cleaned
    .filter(F.col("order_id").isNull())
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        "amount_filled",
        F.max("amount").over(w_grp).alias("last7_max_amount"),
        F.min("amount").over(w_grp).alias("last7_min_amount"),
        F.avg("amount").over(w_grp).alias("last7_avg_amount"),
        F.count("amount").over(w_grp).alias("last7_count_amount"),
    )
)
