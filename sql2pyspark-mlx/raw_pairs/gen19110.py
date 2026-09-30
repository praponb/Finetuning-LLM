from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("monthly_fee_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id", "plan").orderBy("monthly_fee_rounded", "id").rowsBetween(-6, -1)

valid = (
    df_subscriptions
    .filter(F.col("plan") != "premium")
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "status",
        F.round("monthly_fee", 0).alias("monthly_fee_rounded"),
    )
)

result = (
    valid
    .filter(F.col("monthly_fee_rounded").between(40, 75))
    .select(
        "id",
        "plan",
        "monthly_fee_rounded",
        F.sum("monthly_fee_rounded").over(w).alias("running_monthly_fee_rounded"),
        F.avg("monthly_fee_rounded").over(w2).alias("monthly_fee_rounded_avg_6_prior"),
    )
)
