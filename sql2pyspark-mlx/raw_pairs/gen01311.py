from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("opened_date_weekofyear_tier").orderBy(F.col("opened_date_weekofyear_p50").desc())

filtered = (
    df_accounts
    .filter(~F.col("status").eqNullSafe(F.lit("OPEN")))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "status",
        F.initcap(F.trim("status")).alias("status_title"),
        F.weekofyear("opened_date").alias("opened_date_weekofyear"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("opened_date_weekofyear") >= 87, "HIGH").otherwise("LOW").alias("opened_date_weekofyear_tier"),
        "status",
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.count(F.when(F.col("account_type") == "LOAN", 1)).alias("loan_count"),
        F.count_if(F.col("status") == "CLOSED").alias("closed_events"),
        F.percentile_approx("opened_date_weekofyear", 0.5).alias("opened_date_weekofyear_p50"),
        F.stddev_samp("balance").alias("balance_stddev"),
    )
    .filter(F.col("balance_stddev") > 0)
    .withColumn("opened_date_weekofyear_p50_rank", F.rank().over(w))
    .orderBy(F.col("closed_events").desc_nulls_first())
)
