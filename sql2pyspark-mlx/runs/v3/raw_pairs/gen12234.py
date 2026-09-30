from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date", "id")
w2 = Window.partitionBy("order_id").orderBy("cost")

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        "order_id",
        (F.col("cost") - F.lag("cost", 1).over(w)).alias("cost_change"),
        F.rank().over(w2).alias("rnk"),
    )
)
