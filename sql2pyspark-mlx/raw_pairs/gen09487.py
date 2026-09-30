from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "plan",
        "monthly_fee",
        F.max("monthly_fee").over(w).alias("plan_max_monthly_fee"),
    )
    .orderBy(F.col("id").desc(), F.col("plan").desc())
)
