from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("end_date").desc(), "id")

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([8, 25, 37]))
    .select("id", "end_date", F.row_number().over(w).alias("end_date_row"))
)
