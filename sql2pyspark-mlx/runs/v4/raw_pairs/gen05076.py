from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.orderBy(F.col("paid_at").desc())

result = (
    df_payments
    .filter(F.to_date("paid_at") < "2024-09-01")
    .select(
        "id",
        "status",
        "paid_at",
        "method",
        F.last("method").over(w).alias("last_method"),
        F.percent_rank().over(w2).alias("paid_at_pct_rank"),
    )
)
