from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("end_date", "id")

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") <= 100)
    .select(
        "id",
        "customer_id",
        "end_date",
        "monthly_fee",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") == 1)
    .select("id", "customer_id", "end_date", "monthly_fee")
)
