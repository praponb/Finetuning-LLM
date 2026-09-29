from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("delivered_date", "id")

result = (
    df_shipments
    .select(
        "id",
        "delivered_date",
        "cost",
        (F.col("cost") - F.lag("cost", 1).over(w)).alias("cost_change"),
    )
)
