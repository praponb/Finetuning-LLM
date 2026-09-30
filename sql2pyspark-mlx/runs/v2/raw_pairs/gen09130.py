from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("end_date", "id")

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "end_date",
        "monthly_fee",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 1)
    .select("id", "customer_id", "end_date", "monthly_fee")
    .orderBy(F.col("customer_id").desc())
)
