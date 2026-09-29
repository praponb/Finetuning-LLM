from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy("delivered_date")
w2 = Window.partitionBy("status")

result = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "status",
        "order_id",
        "delivered_date",
        "cost",
        F.rank().over(w).alias("delivered_date_rank"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 3).alias("cost_share"),
    )
)
