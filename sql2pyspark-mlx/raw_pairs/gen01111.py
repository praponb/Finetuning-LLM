from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("opened_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_accounts
    .select(
        "id",
        "status",
        "opened_date",
        "customer_id",
        F.last("customer_id").over(w).alias("last_customer_id"),
    )
    .orderBy(F.col("status").desc())
)
