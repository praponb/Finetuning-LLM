from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("end_date", "id")

result = (
    df_subscriptions
    .select("id", "customer_id", "end_date", F.row_number().over(w).alias("row_num"))
    .orderBy(F.col("row_num").desc(), F.col("customer_id").desc())
)
