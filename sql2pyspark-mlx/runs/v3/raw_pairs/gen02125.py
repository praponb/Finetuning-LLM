from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("carrier").orderBy("shipped_date", "id")

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "cost",
        "carrier",
        "shipped_date",
        F.round((F.col("cost") / F.sum("cost").over(w)), 3).alias("cost_share"),
        (F.col("cost") - F.lag("cost", 1, 0).over(w2)).alias("cost_change"),
    )
)
