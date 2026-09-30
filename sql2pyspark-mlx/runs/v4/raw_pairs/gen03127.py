from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("order_date")

result = (
    df_orders
    .filter(F.col("status").isin(["CANCELLED", "COMPLETED"]))
    .select(
        "id",
        "channel",
        "order_date",
        F.percent_rank().over(w).alias("order_date_pct_rank"),
    )
    .orderBy("order_date")
)
