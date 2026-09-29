from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status")

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "status",
        "cost",
        F.round((F.col("cost") / F.sum("cost").over(w)), 3).alias("cost_share"),
    )
)
