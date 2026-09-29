from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("monthly_fee", "id")

result = (
    df_subscriptions
    .select("id", "customer_id", "monthly_fee", F.row_number().over(w).alias("row_num"))
)
