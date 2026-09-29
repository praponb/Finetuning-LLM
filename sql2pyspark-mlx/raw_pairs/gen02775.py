from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy(F.col("opened_date").desc(), "id")

result = (
    df_accounts
    .select(
        "id",
        "opened_date",
        F.first("balance", ignorenulls=True).over(w).alias("first_known_balance"),
        F.row_number().over(w2).alias("event_seq"),
    )
)
