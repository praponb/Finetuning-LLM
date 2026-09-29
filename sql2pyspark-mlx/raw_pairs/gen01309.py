from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("plan").orderBy("monthly_fee", "id")
w2 = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc_nulls_first())

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "status",
        F.lead("monthly_fee").over(w_running).alias("next_monthly_fee"),
        F.row_number().over(w_running).alias("seq_no"),
        F.cume_dist().over(w2).alias("monthly_fee_cume_dist"),
    )
)
