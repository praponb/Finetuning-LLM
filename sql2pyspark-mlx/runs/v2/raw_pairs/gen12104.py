from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("start_date", "id")
w2 = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc(), "id")

result = (
    df_subscriptions
    .filter(F.col("end_date").between("2023-06-01", "2023-08-30"))
    .select(
        "id",
        "start_date",
        "monthly_fee",
        "status",
        (F.col("monthly_fee") - F.lag("monthly_fee", 1).over(w)).alias("monthly_fee_change"),
        F.row_number().over(w2).alias("rn"),
    )
)
