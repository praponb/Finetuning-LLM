from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("end_date")
w2 = Window.partitionBy("customer_id").orderBy("start_date", "id")

result = (
    df_subscriptions
    .filter(F.col("status") == "CANCELLED")
    .select(
        "id",
        "status",
        "end_date",
        "customer_id",
        "start_date",
        F.rank().over(w).alias("rnk"),
        F.ntile(10).over(w2).alias("quartile"),
    )
)
