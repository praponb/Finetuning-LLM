from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_accounts
    .select(
        "*",
        F.last("status", ignorenulls=True).over(w).alias("last_known_status"),
        F.sum("balance").over(w2).alias("cumulative_balance"),
    )
)

result = (
    enriched
    .select(
        "id",
        "balance",
        "opened_date",
        "status",
        "last_known_status",
        "cumulative_balance",
        F.when(F.col("status").rlike("^(FROZEN|CLOSED)$"), "EXTENDED").when(F.col("status").rlike("^(OPEN)$"), "GROUP_B").otherwise("CORE").alias("status_zone"),
    )
    .orderBy(F.col("status").asc_nulls_first(), "balance")
)
