from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("account_type").isin(["CREDIT", "SAVINGS"]) & ((F.year("opened_date") == 2023) | F.col("balance").between(47000, 69000)))
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
    )
    .orderBy("account_type_values")
)
