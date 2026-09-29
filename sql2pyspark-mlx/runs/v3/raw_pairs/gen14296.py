from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("delivered_date", "id")
w2 = Window.partitionBy("order_id").orderBy("shipped_date")

result = (
    df_shipments
    .select(
        "id",
        "status",
        "delivered_date",
        "cost",
        "order_id",
        "shipped_date",
        F.lead("cost", 1).over(w).alias("next_cost"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
)
