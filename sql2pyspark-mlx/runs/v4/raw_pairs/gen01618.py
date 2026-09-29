from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id")
w2 = Window.partitionBy("account_type").orderBy(F.col("opened_date").desc())

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "opened_date",
        "account_type",
        F.lag("opened_date", 1).over(w).alias("prev_opened_date"),
        F.rank().over(w2).alias("opened_date_rank"),
    )
)
