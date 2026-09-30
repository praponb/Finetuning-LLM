from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("order_date", "id")

result = (
    df_orders
    .select(
        "id",
        "store_id",
        "order_date",
        "amount",
        F.lead("amount", 1, 0).over(w).alias("next_amount"),
    )
)
