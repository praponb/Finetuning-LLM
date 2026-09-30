from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("order_date", "id")

result = (
    df_orders
    .select(
        "id",
        "order_date",
        "amount",
        F.lead("amount", 1).over(w).alias("next_amount"),
    )
)
