from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("end_date").desc(), "id")

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "end_date",
        "monthly_fee",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 3)
    .select("id", "customer_id", "end_date", "monthly_fee")
)
