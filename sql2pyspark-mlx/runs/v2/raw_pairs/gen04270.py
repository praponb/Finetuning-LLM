from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("order_date", "id")

result = (
    df_orders
    .filter(F.col("status") == "CANCELLED")
    .select(
        "id",
        "status",
        "order_date",
        "discount",
        F.lead("discount", 2).over(w).alias("next_discount"),
    )
)
