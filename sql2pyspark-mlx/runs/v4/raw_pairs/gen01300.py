from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("end_date", "id")
w2 = Window.partitionBy("status", "plan").orderBy("start_date", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("plan")

scored = (
    df_subscriptions
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("monthly_fee_avg_6_prior", F.avg("monthly_fee").over(w2))
    .withColumn("monthly_fee_share", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w3)), 4))
)

result = (
    scored
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "status",
        "monthly_fee_avg_6_prior",
        "monthly_fee_share",
    )
)
