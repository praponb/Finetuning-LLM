from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("opened_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_accounts
    .select(
        "id",
        "account_type",
        "opened_date",
        "balance",
        F.last("balance").over(w).alias("last_balance"),
    )
)
