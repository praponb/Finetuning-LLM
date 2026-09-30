from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w_part = Window.partitionBy("carrier").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("carrier", "order_id").orderBy(F.col("cost").desc(), "id")
w4 = Window.partitionBy("order_id")

result = (
    df_shipments
    .select(
        "*",
        F.round((F.col("cost") / F.sum("cost").over(w)), 4).alias("cost_share"),
        F.round((F.col("cost") / F.sum("cost").over(w)), 4).alias("cost_share_2"),
        F.last("carrier", ignorenulls=True).over(w_part).alias("last_known_carrier"),
        F.ntile(10).over(w3).alias("cost_bucket"),
        F.round((F.col("cost") / F.sum("cost").over(w4)), 4).alias("cost_share_3"),
    )
)
