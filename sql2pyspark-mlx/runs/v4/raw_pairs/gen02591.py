from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy(F.col("monthly_fee").desc(), "id")
w2 = Window.partitionBy("customer_id", "status").orderBy("monthly_fee", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("end_date_weekofyear", "id").rowsBetween(-6, Window.currentRow)

base = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "monthly_fee",
        "status",
        F.weekofyear("end_date").alias("end_date_weekofyear"),
    )
)

result = (
    base
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.avg("end_date_weekofyear").over(w2).alias("end_date_weekofyear_avg_2"),
        F.avg("monthly_fee").over(w3).alias("monthly_fee_avg_6"),
    )
)
