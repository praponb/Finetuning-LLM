from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("customer_id").orderBy("end_date", "id")
w3 = Window.partitionBy("status").orderBy("end_date", "id")
w4 = Window.partitionBy(F.month("end_date")).orderBy(F.col("end_date").desc(), "id")
w5 = Window.partitionBy("customer_id")

enriched = (
    df_subscriptions
    .select(
        "*",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 4).alias("monthly_fee_share"),
        F.lead("monthly_fee", 2).over(w2).alias("next_monthly_fee"),
        F.row_number().over(w3).alias("seq_no"),
        F.row_number().over(w4).alias("row_seq"),
        F.min("monthly_fee").over(w5).alias("customer_id_min_monthly_fee"),
    )
)

result = (
    enriched
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        "monthly_fee_share",
        "row_seq",
        "customer_id_min_monthly_fee",
    )
    .orderBy(F.col("end_date").desc_nulls_last(), "id")
)
