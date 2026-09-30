from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("start_date")
w2 = Window.partitionBy("customer_id", "plan").orderBy("start_date")

result = (
    df_subscriptions
    .filter(F.col("start_date").between("2023-08-08", "2025-08-01"))
    .select(
        "id",
        "status",
        "start_date",
        "customer_id",
        "plan",
        F.dense_rank().over(w).alias("start_date_dense_rank"),
        F.rank().over(w2).alias("start_date_rank"),
    )
    .orderBy("plan")
)
