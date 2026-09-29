from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("days_since_opened_date").asc_nulls_first())
w_ordered = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("days_since_opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status", "account_type").orderBy(F.col("balance").desc_nulls_first(), "id")

valid = (
    df_accounts
    .filter(F.col("status").isin(["FROZEN", "OPEN"]))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.cume_dist().over(w).alias("days_since_opened_date_cume_dist"),
        F.sum("days_since_opened_date").over(w_ordered).alias("running_days_since_opened_date"),
        F.sum("days_since_opened_date").over(w3).alias("running_days_since_opened_date_2"),
        F.sum("balance").over(w4).alias("running_balance"),
    )
)

latest = (
    metrics
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .select(
        "id",
        "status",
        F.col("days_since_opened_date_cume_dist").alias("DAYS_SINCE_OPENED_DATE_CUME_DIST"),
        F.col("balance").alias("BALANCE_VAL"),
    )
    .unpivot(
        ["id", "status"],
        ["DAYS_SINCE_OPENED_DATE_CUME_DIST", "BALANCE_VAL"],
        "metric",
        "val",
    )
    .filter(F.col("val").isNotNull())
)
