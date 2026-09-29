from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("start_date", "id")

result = (
    df_subscriptions
    .filter(~F.col("plan").isin(["premium", "enterprise"]))
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 3)
    .orderBy("start_date")
)
