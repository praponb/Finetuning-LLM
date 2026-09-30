from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") < 76000) & (F.col("status") == "CLOSED") & (F.col("balance") <= 46000))
    .cube(F.date_trunc("MONTH", "opened_date").alias("opened_month"), "customer_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.count("balance").alias("balance_count"),
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_count"),
    )
    .orderBy("open_count")
)
