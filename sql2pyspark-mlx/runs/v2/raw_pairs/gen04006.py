from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("delivered_date", "id")

result = (
    df_shipments
    .select("id", "delivered_date", "cost", F.lag("cost", 2).over(w).alias("prev_cost"))
)
