from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id")

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "start_date",
        "monthly_fee",
        "customer_id",
        F.sum("monthly_fee").over(w).alias("running_monthly_fee"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 3).alias("monthly_fee_share"),
    )
)
