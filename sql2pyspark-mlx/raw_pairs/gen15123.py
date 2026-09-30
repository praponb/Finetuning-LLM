from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("balance", "id")
w2 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_accounts
    .filter(F.col("status") == "OPEN")
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.lower("account_type").alias("account_type_lower"),
    )
)

result = (
    filtered
    .select(
        "id",
        "customer_id",
        "opened_date",
        "account_type_lower",
        F.lead("balance").over(w).alias("next_balance"),
        F.row_number().over(w).alias("seq_no"),
        F.sum("balance").over(w2).alias("running_balance"),
    )
)
