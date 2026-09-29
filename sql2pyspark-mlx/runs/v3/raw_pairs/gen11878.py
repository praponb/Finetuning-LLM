from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "customer_id").orderBy(F.col("monthly_fee").desc(), "id")

result = (
    df_subscriptions
    .filter(F.col("customer_id") == 6)
    .select(
        "id",
        "plan",
        "customer_id",
        "monthly_fee",
        F.row_number().over(w).alias("rn"),
    )
    .orderBy(F.col("monthly_fee").desc(), "plan")
)
