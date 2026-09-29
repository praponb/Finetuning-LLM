from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")

result = (
    df_shipments
    .filter(F.col("cost") > 61)
    .select(
        "id",
        "order_id",
        "cost",
        F.round((F.col("cost") / F.sum("cost").over(w)), 3).alias("cost_share"),
    )
)
