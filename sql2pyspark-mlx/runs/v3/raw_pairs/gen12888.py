from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "carrier")

result = (
    df_shipments
    .filter(~F.col("status").isin(["DELIVERED", "LOST"]))
    .select(
        "id",
        "order_id",
        "carrier",
        "cost",
        F.round((F.col("cost") / F.sum("cost").over(w)), 3).alias("cost_share"),
    )
)
