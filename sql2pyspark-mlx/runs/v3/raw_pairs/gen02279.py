from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("delivered_date", "id")

result = (
    df_shipments
    .filter(~F.col("status").isin(["DELIVERED", "IN_TRANSIT"]))
    .select("id", "delivered_date", "cost", F.lag("cost", 1).over(w).alias("prev_cost"))
    .orderBy(F.col("id").desc())
)
