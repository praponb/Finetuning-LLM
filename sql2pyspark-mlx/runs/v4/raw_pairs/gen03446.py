from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "channel").orderBy(F.col("amount").desc())
w2 = Window.partitionBy("store_id").orderBy("order_date")

result = (
    df_orders
    .filter(F.col("amount") <= 1160)
    .select(
        "id",
        "store_id",
        "channel",
        "amount",
        "order_date",
        F.percent_rank().over(w).alias("amount_pct_rank"),
        F.rank().over(w2).alias("rnk"),
    )
)
