from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(~F.col("c.country").isin(["UK", "US", "TH"]))
    .groupBy(
        F.date_format(F.col("c.signup_date"), "yyyy-MM").alias("signup_month"),
        F.year(F.col("c.signup_date")).alias("signup_year"),
    )
    .agg(
        F.avg(F.col("a.balance")).alias("avg_balance"),
        F.sort_array(F.collect_set(F.col("a.status"))).alias("status_list"),
    )
    .filter(F.col("avg_balance") > 68000)
)
