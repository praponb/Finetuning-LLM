from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("paid_at").desc_nulls_last(), "id")

ranked = (
    df_payments
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        "latest_rank",
        F.when(F.col("method").rlike("^(cash|card)$"), "PRIORITY").otherwise("GROUP_A").alias("method_zone"),
    )
    .orderBy("latest_rank")
)
