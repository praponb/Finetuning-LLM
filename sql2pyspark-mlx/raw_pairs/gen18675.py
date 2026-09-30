from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("shipped_date"), "status").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy(F.col("delivered_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_shipments
    .filter(F.col("cost") > 90)
    .select(
        "id",
        "shipped_date",
        "status",
        "delivered_date",
        "cost",
        "order_id",
        "carrier",
        F.sum("cost").over(w).alias("running_cost"),
        F.last("carrier").over(w2).alias("last_carrier"),
    )
    .orderBy("shipped_date")
)
