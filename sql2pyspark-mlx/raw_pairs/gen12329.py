from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("delivered_date", "id")
w2 = Window.partitionBy("carrier").orderBy("shipped_date", "id")

result = (
    df_shipments
    .select(
        "id",
        "status",
        "delivered_date",
        "cost",
        "carrier",
        "shipped_date",
        F.lead("cost", 2).over(w).alias("next_cost"),
        F.lag("cost", 1).over(w2).alias("prev_cost"),
    )
    .orderBy("id")
)
