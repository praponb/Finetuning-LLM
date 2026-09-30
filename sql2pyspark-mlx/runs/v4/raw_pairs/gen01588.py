from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("order_id", "status")
w3 = Window.partitionBy("method").orderBy(F.col("amount").desc_nulls_first())

enriched = (
    df_payments
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w)), 4))
    .withColumn("order_id_avg_amount", F.avg("amount").over(w2))
    .withColumn("amount_dense_rank", F.dense_rank().over(w3))
)

result = (
    enriched
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        "order_id_avg_amount",
        "amount_dense_rank",
        F.when(F.col("status").rlike("^(SUCCESS|REFUNDED)$"), "GROUP_A").when(F.col("status").rlike("^(FAILED)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("status_zone"),
    )
)
