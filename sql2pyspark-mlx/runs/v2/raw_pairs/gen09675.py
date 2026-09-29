from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("delivered_date", "id")
w2 = Window.partitionBy("order_id").orderBy("shipped_date", "id")

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "delivered_date",
        "cost",
        "order_id",
        "shipped_date",
        F.sum("cost").over(w).alias("running_cost"),
        F.sum("cost").over(w2).alias("running_cost_2"),
    )
    .orderBy(F.col("running_cost_2").desc())
)
