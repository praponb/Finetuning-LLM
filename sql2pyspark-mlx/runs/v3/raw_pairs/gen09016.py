from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "carrier").orderBy("delivered_date", "id")
w2 = Window.partitionBy("status").orderBy("shipped_date")

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        "shipped_date",
        F.lead("cost", 1).over(w).alias("next_cost"),
        F.rank().over(w2).alias("rnk"),
    )
    .orderBy(F.col("next_cost").desc())
)
