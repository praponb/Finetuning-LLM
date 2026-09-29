from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("monthly_fee", "id")
w_grp = Window.partitionBy("customer_id").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("start_date", "id").rowsBetween(-6, Window.currentRow)
w5 = Window.orderBy("start_date", "id")

scored = (
    df_subscriptions
    .filter(F.col("monthly_fee") < 160)
    .select(
        "*",
        F.first("monthly_fee", ignorenulls=True).over(w).alias("first_known_monthly_fee"),
        F.lag("monthly_fee", 1, 0).over(w2).alias("prev_monthly_fee"),
        F.last("monthly_fee", ignorenulls=True).over(w_grp).alias("last_known_monthly_fee"),
        F.avg("monthly_fee").over(w4).alias("monthly_fee_avg_6"),
        F.lag("monthly_fee", 1).over(w5).alias("prev_monthly_fee_2"),
    )
)

result = (
    scored
    .select(
        "id",
        "start_date",
        "end_date",
        "prev_monthly_fee",
        "last_known_monthly_fee",
        "monthly_fee_avg_6",
        "prev_monthly_fee_2",
    )
    .orderBy(F.col("start_date").asc_nulls_first())
)
