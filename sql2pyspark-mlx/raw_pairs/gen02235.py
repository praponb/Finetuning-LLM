from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("start_date", "id")

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "start_date",
        "customer_id",
        F.first("customer_id").over(w).alias("first_customer_id"),
    )
)
