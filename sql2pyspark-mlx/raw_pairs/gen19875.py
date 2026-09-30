from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy("end_date", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc_nulls_last())
w3 = Window.partitionBy("plan").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_subscriptions
    .select(
        "id",
        "start_date",
        "end_date",
        "status",
        F.avg("monthly_fee").over(w).alias("monthly_fee_avg_6_prior"),
        F.rank().over(w2).alias("monthly_fee_rank"),
        F.sum("monthly_fee").over(w3).alias("cumulative_monthly_fee"),
    )
)

result = (
    enriched
    .select("id", "start_date", "monthly_fee_avg_6_prior", "cumulative_monthly_fee")
)
