from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("start_date").desc(), "id")

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([10, 18, 30]))
    .select(
        "id",
        "status",
        "start_date",
        "monthly_fee",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 2)
    .select("id", "status", "start_date", "monthly_fee")
)
