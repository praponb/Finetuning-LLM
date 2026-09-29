from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("start_date", "id")

result = (
    df_subscriptions
    .select("id", "start_date", F.ntile(10).over(w).alias("start_date_ntile"))
)
