from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("shipped_date", "id")

result = (
    df_shipments
    .filter(F.col("order_id") == 12)
    .select("id", "shipped_date", "cost", F.lag("cost", 1).over(w).alias("prev_cost"))
)
