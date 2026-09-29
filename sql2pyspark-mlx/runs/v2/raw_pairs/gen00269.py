from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("status").orderBy("delivered_date", "id")

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "cost",
        "status",
        "delivered_date",
        F.round((F.col("cost") / F.sum("cost").over(w)), 3).alias("cost_share"),
        F.sum("cost").over(w2).alias("running_cost"),
    )
)
