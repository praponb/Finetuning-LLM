from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc_nulls_last())

deduped = (
    df_payments
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        "dedup_rank",
        F.when(F.col("status").rlike("^(SUCCESS|FAILED)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("status_zone"),
    )
    .orderBy(F.col("status_zone").desc())
)
