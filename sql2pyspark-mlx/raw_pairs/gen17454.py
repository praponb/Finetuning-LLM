from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("delivered_date")

result = (
    df_shipments
    .filter(F.col("order_id") == 16)
    .select(
        "id",
        "order_id",
        "shipped_date",
        "cost",
        "status",
        "delivered_date",
        F.sum("cost").over(w).alias("running_cost"),
        F.rank().over(w2).alias("rnk"),
    )
    .orderBy(F.col("running_cost").desc())
)
