from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("carrier")
w2 = Window.partitionBy("status", "order_id")
w_running = Window.partitionBy(F.month("delivered_date"), "status").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        F.stddev_samp("cost").over(w_recent).alias("carrier_stddev_samp_cost"),
        F.stddev_samp("cost").over(w2).alias("status_stddev_samp_cost"),
        F.last("carrier", ignorenulls=True).over(w_running).alias("last_known_carrier"),
    )
)
