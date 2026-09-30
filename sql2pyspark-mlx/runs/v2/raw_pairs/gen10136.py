from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("delivered_date", "id")
w2 = Window.partitionBy("status", "carrier")

result = (
    df_shipments
    .filter(~F.col("carrier").isin(["DHL", "Flash", "Kerry"]))
    .select(
        "id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        F.lead("cost", 1).over(w).alias("next_cost"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 3).alias("cost_share"),
    )
    .orderBy("carrier", "delivered_date")
)
