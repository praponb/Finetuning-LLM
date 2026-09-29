from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("start_date").desc(), "id")

result = (
    df_subscriptions
    .filter(F.col("status") == "ACTIVE")
    .select("id", "customer_id", "start_date", F.row_number().over(w).alias("row_num"))
    .orderBy(F.col("row_num").desc())
)
