from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("delivered_date")

result = (
    df_shipments
    .select("id", "delivered_date", F.rank().over(w).alias("delivered_date_rank"))
)
