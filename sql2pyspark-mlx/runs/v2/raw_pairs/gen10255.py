from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "order_id")
w2 = Window.partitionBy("order_id").orderBy("shipped_date", "id")

result = (
    df_shipments
    .filter(F.col("status").isin(["LOST", "DELIVERED"]))
    .select(
        "id",
        "carrier",
        "order_id",
        "cost",
        "shipped_date",
        F.min("cost").over(w).alias("carrier_min_cost"),
        F.lead("cost", 1, 0).over(w2).alias("next_cost"),
    )
)
