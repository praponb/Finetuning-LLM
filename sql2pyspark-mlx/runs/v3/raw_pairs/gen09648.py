from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "channel").orderBy("order_date", "id")

result = (
    df_orders
    .filter(F.col("discount") <= 0.26)
    .select(
        "id",
        "status",
        "channel",
        "order_date",
        "amount",
        F.lead("amount", 1).over(w).alias("next_amount"),
    )
)
