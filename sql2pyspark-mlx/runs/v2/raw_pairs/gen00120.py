from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNull())
    .cube(F.month("opened_date").alias("opened_month"), "account_type")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.countDistinct("balance").alias("unique_statuss"),
    )
    .orderBy(F.col("account_type").desc(), F.col("opened_month").desc())
)
