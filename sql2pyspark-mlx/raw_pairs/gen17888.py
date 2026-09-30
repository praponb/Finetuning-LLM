from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("end_date").desc(), "id")

result = (
    df_subscriptions
    .filter(F.year("start_date") >= 2023)
    .select("id", "customer_id", "end_date", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 1)
    .orderBy(F.col("customer_id").desc(), "id")
)
