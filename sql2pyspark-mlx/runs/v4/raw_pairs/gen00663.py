from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.month("opened_date").isin([2, 6, 8]) & F.col("customer_id").isin([14, 25]))
    .groupBy("status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .orderBy("status", F.col("status_values").desc())
)
