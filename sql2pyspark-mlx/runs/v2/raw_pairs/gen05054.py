from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~(F.col("monthly_fee").between(120, 200) & ((F.col("status") == "EXPIRED") | (F.col("monthly_fee") <= 240))))
    .groupBy("status")
    .agg(
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
    )
)
