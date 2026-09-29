from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id")
w2 = Window.partitionBy("customer_id")

enriched = (
    df_subscriptions
    .filter(F.col("status").isin(["CANCELLED", "PAUSED"]))
    .select(
        "id",
        "plan",
        "end_date",
        "status",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 4).alias("monthly_fee_share"),
        F.stddev_samp("monthly_fee").over(w2).alias("customer_id_stddev_samp_monthly_fee"),
    )
)

result = (
    enriched
    .select("id", "end_date")
    .orderBy(F.col("id").asc_nulls_first(), F.col("end_date").desc())
    .limit(5)
)
