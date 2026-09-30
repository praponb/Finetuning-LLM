from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date", "id")
w2 = Window.partitionBy("order_id").orderBy("delivered_date", "id")

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        "order_id",
        "delivered_date",
        F.sum("cost").over(w).alias("running_cost"),
        F.lead("cost", 1, 0).over(w2).alias("next_cost"),
    )
)
