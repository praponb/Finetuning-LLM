from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("balance").desc(), "id")
w2 = Window.partitionBy("customer_id").orderBy("opened_date", "id")

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        F.row_number().over(w).alias("rn"),
        F.lag("balance", 1).over(w2).alias("prev_balance"),
    )
)
