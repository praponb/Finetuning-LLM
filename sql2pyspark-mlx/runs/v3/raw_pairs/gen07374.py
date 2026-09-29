from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("order_date", "id")
w2 = Window.partitionBy("status")

result = (
    df_orders
    .select(
        "id",
        "status",
        "order_date",
        "discount",
        F.lead("discount", 1, 0).over(w).alias("next_discount"),
        F.avg("discount").over(w2).alias("status_avg_discount"),
    )
)
