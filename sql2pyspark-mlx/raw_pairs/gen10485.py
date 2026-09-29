from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist_2 = Window.partitionBy("carrier", "status").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("carrier").orderBy("cost")

enriched = (
    df_shipments
    .select(
        "id",
        "carrier",
        "cost",
        "status",
        F.sum("cost").over(w_hist).alias("running_cost"),
        F.last("cost", ignorenulls=True).over(w_hist_2).alias("last_known_cost"),
        F.rank().over(w3).alias("cost_rank"),
    )
)

result = enriched.select("id", "running_cost")
