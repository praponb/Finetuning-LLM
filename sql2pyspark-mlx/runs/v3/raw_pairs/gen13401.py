from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
    )
)
