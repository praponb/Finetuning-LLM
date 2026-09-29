from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc(), "id")
w2 = Window.partitionBy("status")

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "monthly_fee",
        "plan",
        "status",
        F.first("plan").over(w).alias("first_plan"),
        F.count("*").over(w2).alias("status_count"),
    )
)
