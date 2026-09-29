from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("delivered_date", "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("delivered_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_shipments
    .filter(F.year("delivered_date") == 2023)
    .select(
        "id",
        "order_id",
        "delivered_date",
        "shipped_date",
        F.row_number().over(w).alias("rn"),
        F.last("shipped_date").over(w2).alias("last_shipped_date"),
    )
)
