from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_accounts
    .filter(F.col("status").isin(["FROZEN", "OPEN", "CLOSED"]))
    .select(
        "id",
        "status",
        "balance",
        "opened_date",
        F.last("opened_date").over(w).alias("last_opened_date"),
    )
)
