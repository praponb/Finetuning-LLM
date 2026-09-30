from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.to_date("paid_at") < "2023-10-01")
    .groupBy(
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
        "order_id",
    )
    .agg(
        F.min("paid_at").alias("first_paid_at"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .orderBy("paid_cohort")
)
