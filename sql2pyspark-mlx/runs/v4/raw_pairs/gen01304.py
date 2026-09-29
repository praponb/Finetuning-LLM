from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("start_date", "id")

result = (
    df_subscriptions
    .filter(F.col("customer_id") == 27)
    .select("id", "status", "start_date", F.row_number().over(w).alias("rn"))
)
