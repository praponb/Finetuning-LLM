from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("opened_date") < "2024-08-01") | ~F.col("status").isin(["CLOSED", "OPEN"]))
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        (F.max("balance") - F.min("balance")).alias("balance_range"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
    )
    .orderBy("opened_cohort")
    .limit(50)
)
