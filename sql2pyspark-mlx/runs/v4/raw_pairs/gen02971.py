from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_subscriptions
    .filter(F.col("start_date").between("2023-06-01", "2024-11-01"))
    .select(
        "id",
        "customer_id",
        "monthly_fee",
        "end_date",
        F.last("end_date").over(w).alias("last_end_date"),
    )
)
