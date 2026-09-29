from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("start_date").desc(), "id")

result = (
    df_subscriptions
    .filter(F.col("start_date") > "2025-06-11")
    .select("id", "customer_id", "start_date", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
    .select("id", "customer_id", "start_date")
    .orderBy("start_date")
)
