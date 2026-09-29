from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("opened_date"), "account_type").orderBy(F.col("balance").desc_nulls_first())
w2 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("balance", "id")
w4 = Window.partitionBy("status", "account_type").orderBy("opened_date", "id")

enriched = (
    df_accounts
    .select(
        "*",
        F.dense_rank().over(w).alias("balance_dense_rank"),
        F.first("status", ignorenulls=True).over(w2).alias("first_known_status"),
        F.lag("balance", 2).over(w3).alias("prev_balance"),
        F.lag("balance", 1).over(w4).alias("prev_balance_2"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("prev_balance") >= 86000, "LARGE").otherwise("SMALL").alias("prev_balance_tier"),
    )
    .agg(
        F.percentile_approx("prev_balance_2", [0.25, 0.5, 0.75]).alias("prev_balance_2_quartiles"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.size(F.collect_set("status")).alias("num_status"),
    )
    .filter(F.col("num_status") >= 1)
    .orderBy(F.col("prev_balance_tier").desc_nulls_last())
)
