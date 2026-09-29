from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("opened_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_accounts
    .select(
        "id",
        "opened_date",
        "status",
        F.last("status").over(w).alias("last_status"),
    )
    .orderBy(F.col("status").desc(), "opened_date")
)
