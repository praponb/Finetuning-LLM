from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method")
w2 = Window.partitionBy("order_id").orderBy("amount", "id")

enriched = (
    df_payments
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w)), 4))
    .withColumn("amount_bucket", F.ntile(4).over(w2))
)

result = (
    enriched
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        "amount_share",
        "amount_bucket",
    )
)
