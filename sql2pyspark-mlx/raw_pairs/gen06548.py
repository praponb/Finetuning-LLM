from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("end_date"), "customer_id").orderBy("monthly_fee", "id")
w2 = Window.partitionBy("customer_id")

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "end_date",
        "status",
        F.lead("monthly_fee", 1).over(w).alias("next_monthly_fee"),
        F.max("monthly_fee").over(w2).alias("customer_id_max_monthly_fee"),
    )
)
