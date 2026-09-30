from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "customer_id").orderBy(F.col("amount").desc())

result = (
    df_orders
    .select(
        "id",
        "channel",
        "customer_id",
        "amount",
        F.percent_rank().over(w).alias("amount_pct_rank"),
    )
)
