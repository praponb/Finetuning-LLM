from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~((F.col("opened_date") <= "2025-01-01") & ~F.col("account_type").isin(["CHECKING", "SAVINGS"])))
    .agg(
        F.count("*").alias("num_accounts"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
    )
)
