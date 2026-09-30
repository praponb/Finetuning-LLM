from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("opened_date", "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("opened_date").desc(), "id")

result = (
    df_accounts
    .select(
        "id",
        "status",
        "opened_date",
        "balance",
        "customer_id",
        F.lead("balance", 1, 0).over(w).alias("next_balance"),
        F.row_number().over(w2).alias("rn"),
    )
)
