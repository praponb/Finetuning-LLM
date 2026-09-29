from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "plan").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("start_date", "id")

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([22, 29, 30, 32]))
    .select(
        "id",
        "status",
        "plan",
        "end_date",
        "monthly_fee",
        "start_date",
        F.sum("monthly_fee").over(w).alias("running_monthly_fee"),
        F.row_number().over(w2).alias("rn"),
    )
    .orderBy(F.col("id").desc())
)
