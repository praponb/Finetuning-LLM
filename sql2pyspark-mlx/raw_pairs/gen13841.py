from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("end_date"), "customer_id").orderBy(F.col("monthly_fee").desc(), "id")
w2 = Window.partitionBy("status").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_subscriptions
    .select(
        "*",
        F.ntile(5).over(w).alias("monthly_fee_bucket"),
        F.sum("monthly_fee").over(w2).alias("cumulative_monthly_fee"),
    )
)

result = (
    metrics
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "cumulative_monthly_fee",
    )
    .orderBy(F.col("plan").desc_nulls_first(), F.col("monthly_fee").desc())
)
