from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("plan").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("monthly_fee").desc())

cleaned = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.when(F.col("plan").isin(["premium", "enterprise"]), "Priority").otherwise("Other").alias("plan_group"),
    )
)

result = (
    cleaned
    .select(
        "*",
        F.first("monthly_fee", ignorenulls=True).over(w).alias("first_known_monthly_fee"),
        F.last("monthly_fee", ignorenulls=True).over(w2).alias("last_known_monthly_fee"),
        F.cume_dist().over(w3).alias("monthly_fee_cume_dist"),
    )
)
