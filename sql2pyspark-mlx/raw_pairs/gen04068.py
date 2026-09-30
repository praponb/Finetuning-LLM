from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy(F.month("delivered_date")).orderBy("shipped_date", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_shipments
    .filter(F.col("carrier").isin(["UPS", "FedEx"]))
    .select(
        "id",
        "status",
        "cost",
        "delivered_date",
        "shipped_date",
        F.round((F.col("cost") / F.sum("cost").over(w)), 3).alias("cost_share"),
        F.round(F.avg("cost").over(w2), 2).alias("cost_moving_avg_7"),
    )
    .orderBy("cost_share")
)
