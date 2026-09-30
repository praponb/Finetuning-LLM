from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("discount", "id")

result = (
    df_orders
    .select("id", "discount", "status", F.first("status").over(w).alias("first_status"))
)
