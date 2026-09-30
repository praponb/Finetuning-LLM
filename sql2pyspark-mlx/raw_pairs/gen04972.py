from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy("end_date", "id")

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") < 180)
    .select(
        "id",
        "plan",
        "start_date",
        "monthly_fee",
        "status",
        F.lead("monthly_fee").over(w).alias("next_monthly_fee"),
        F.row_number().over(w).alias("row_seq"),
    )
)
