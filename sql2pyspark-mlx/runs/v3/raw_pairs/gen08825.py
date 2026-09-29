from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("end_date", "id")

result = (
    df_subscriptions
    .filter(F.col("status").isin(["CANCELLED", "EXPIRED"]))
    .select(
        "id",
        "plan",
        "end_date",
        "customer_id",
        F.first("customer_id").over(w).alias("first_customer_id"),
    )
)
