from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("end_date", "id")

result = (
    df_subscriptions
    .select("id", "customer_id", "end_date", "plan", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "customer_id", "end_date", "plan")
    .orderBy("plan")
)
