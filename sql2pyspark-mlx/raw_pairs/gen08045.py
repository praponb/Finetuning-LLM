from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("customer_id").orderBy("end_date", "id")

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") < 130)
    .select(
        "id",
        "customer_id",
        "end_date",
        "monthly_fee",
        F.count("*").over(w).alias("customer_id_count"),
        F.lead("monthly_fee", 2).over(w2).alias("next_monthly_fee"),
    )
)
