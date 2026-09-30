from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("start_date", "id")

result = (
    df_subscriptions
    .select("id", "start_date", F.row_number().over(w).alias("start_date_row"))
)
