from pyspark.sql import functions as F

cleaned = (
    df_accounts
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-09-03"))) & (F.col("status") == "CLOSED"))
    .withColumn("account_type_status", F.concat("account_type", F.lit(" "), "status"))
)

result = (
    cleaned
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .cube(F.col("p.country").alias("country"), F.col("src.status").alias("status"))
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("src.balance")).alias("max_balance"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("num_rows").desc_nulls_first())
)
